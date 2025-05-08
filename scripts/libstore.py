import os
import asyncio
import argparse
import json
import nekoton as nt
from typing import Optional

dirname = os.path.dirname(__file__)


def parse_cell(boc: str) -> nt.Cell:
    return nt.Cell.decode(boc, "base64")


def parse_cell_or_hash(hash: str) -> bytes:
    try:
        cell = nt.Cell.decode(hash, "base64")
        return cell.repr_hash
    except Exception:
        pass

    if len(hash) != 64:
        raise ValueError("expected a hex-encoded library hash")
    return bytes.fromhex(hash)


class Action:
    CHANGE_LIB_TAG = 0x26FA1DD4

    def store_into(self, builder: nt.CellBuilder):
        raise NotImplementedError("store_into must be implemented")


class AddLib(Action):
    def __init__(self, code: nt.Cell):
        self.code = code

    def store_into(self, builder: nt.CellBuilder):
        builder.store_u32(Action.CHANGE_LIB_TAG)
        builder.store_u8(5)
        builder.store_reference(self.code)

    def __repr__(self):
        return f"AddLib({self.code.repr_hash.hex()})"


class RemoveLib(Action):
    def __init__(self, code: nt.Cell | bytes):
        if isinstance(code, nt.Cell):
            code = code.repr_hash
        self.hash = code

    def store_into(self, builder: nt.CellBuilder):
        builder.store_u32(Action.CHANGE_LIB_TAG)
        builder.store_u8(0)
        builder.store_bytes(self.hash)

    def __repr__(self):
        return f"RemoveLib({self.hash.hex()})"


class AppendAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if "lib_actions" not in namespace:
            setattr(namespace, "lib_actions", [])
        previous = namespace.lib_actions
        match self.dest:
            case "add":
                previous.append(AddLib(parse_cell(values)))
            case "remove":
                previous.append(RemoveLib(parse_cell_or_hash(values)))
            case _:
                raise ValueError(f"Unknown action {self.dest}")

        setattr(namespace, "lib_actions", previous)


parser = argparse.ArgumentParser(description="Manage Onchain Libraries")
parser.add_argument_group()
parser.add_argument(
    "--rpc", required=False, help="JRPC endpoint URL", default="http://127.0.0.1:8001"
)

common_parser = argparse.ArgumentParser(add_help=False)
common_parser.add_argument(
    "--sign",
    required=False,
    help="Path to the giver keys",
    default=os.path.join(dirname, "../keys.json"),
)


subparsers = parser.add_subparsers(required=True, dest="subparser_name")

cmd_address = subparsers.add_parser(
    "address", help="Get libstore address", parents=[common_parser]
)

cmd_add = subparsers.add_parser(
    "update", help="Update libraries", parents=[common_parser]
)
cmd_add.add_argument("-a", "--add", help="Add library", action=AppendAction)
cmd_add.add_argument("-r", "--remove", help="Remove library", action=AppendAction)
cmd_add.add_argument(
    "--giver",
    help="Use giver",
    type=nt.Address,
    default=nt.Address(
        "-1:1111111111111111111111111111111111111111111111111111111111111111"
    ),
)

args = parser.parse_args()

libstore_code: nt.Cell = nt.Asm.compile("""
NOP
EQINT -1 PUSHSLICE { ACCEPT } BLESS IFNOTJMP
DUP // body body
LDREF // body args body'
LDU 64 // body args timestamp body'
PLDU 32 // body args timestamp expire_at
NOW LESS THROWIF 57 // body args timestamp
DUP NOW INT 1800 ADD INT 1000 MUL GREATER THROWIF 52 // body args timestamp
PUSH c4 CTOS // body args timestamp c4
LDU 256 // body args timestamp pubkey c4'
PLDU 64 // body args timestamp pubkey prev_timestamp
PUSH s2 // body args timestamp pubkey prev_timestamp timestamp
GEQ THROWIF 52 // body args timestamp pubkey
ROLL 3 // args timestamp pubkey body
DUP INT 512 SDCUTLAST // args timestamp pubkey body signature
SWAP INT 512 SDSKIPLAST // args timestamp pubkey signature body_wihtout_signature
NEWC STSLICE MYADDR STSLICER ENDC HASHCU // args timestamp pubkey signature hash
SWAP PUSH s2 // args timestamp pubkey hash signature pubkey
CHKSIGNU THROWIFNOT 40 // args timestamp pubkey
NEWC STU 256 STU 64 ENDC POP c4 // args
POP c5
ACCEPT
""")

# print(libstore_code.encode())


class LibStore:
    @classmethod
    def compute_address(
        cls,
        public_key: nt.PublicKey,
    ) -> nt.Address:
        return cls.compute_state_init(public_key).compute_address(-1)

    @staticmethod
    def compute_state_init(public_key: nt.PublicKey) -> nt.StateInit:
        data_builder = nt.CellBuilder()
        data_builder.store_public_key(public_key)
        data_builder.store_u64(0)
        return nt.StateInit(libstore_code, data_builder.build())

    @staticmethod
    def build_actions(actions: list[Action]) -> nt.Cell:
        result = nt.Cell()
        for action in reversed(actions):
            builder = nt.CellBuilder()
            builder.store_reference(result)
            action.store_into(builder)
            result = builder.build()
        return result

    def __init__(self, transport: nt.Transport, keypair=nt.KeyPair):
        state_init = self.compute_state_init(keypair.public_key)

        self._initialized = False
        self._transport = transport
        self._keypair = keypair
        self._state_init = state_init
        self._address = state_init.compute_address(-1)

    @property
    def address(self) -> nt.Address:
        return self._address

    async def send(self, actions: list[Action]) -> nt.Transaction:
        if len(actions) > 254:
            raise ValueError("Too many actions")
        return await self.send_raw(LibStore.build_actions(actions))

    async def send_raw(
        self,
        actions: nt.Cell,
    ) -> nt.Transaction:
        state_init = await self.__get_state_init()

        signature_id = await self._transport.get_signature_id()

        now_ms = self._transport.clock.now_ms
        expire_at = now_ms // 1000 + 40

        body = nt.CellBuilder()
        body.store_u64(now_ms)
        body.store_u32(expire_at)
        body.store_reference(actions)

        body_to_sign = nt.CellBuilder()
        body_to_sign.store_builder(body)
        body_to_sign.store_uint(4, 3)
        body_to_sign.store_i8(self._address.workchain)
        body_to_sign.store_bytes(self._address.account)
        hash_to_sign = body_to_sign.build().repr_hash

        signature = self._keypair.sign_raw(hash_to_sign, signature_id)
        body.store_bytes(signature.to_bytes())

        external_message = nt.SignedExternalMessage(
            self._address,
            expire_at,
            body=body.build(),
            state_init=state_init,
        )

        tx = await self._transport.send_external_message(external_message)
        if tx is None:
            raise RuntimeError("Message expired")
        return tx

    async def get_account_state(self) -> Optional[nt.AccountState]:
        return await self._transport.get_account_state(self._address)

    async def __get_state_init(self) -> Optional[nt.StateInit]:
        if self._initialized:
            return None

        account_state = await self.get_account_state()
        if (
            account_state is not None
            and account_state.status == nt.AccountStatus.Active
        ):
            self._initialized = True
            return None
        else:
            return self._state_init


async def main():
    keypair = load_keys(args.sign)

    match args.subparser_name:
        case "address":
            address = LibStore.compute_address(keypair.public_key)
            print(address)
            pass

        case "update":
            if args.lib_actions is None:
                args.lib_actions = []

            if len(args.lib_actions) == 0:
                print("actions: []")
                return

            print("actions: [")
            for action in args.lib_actions:
                print(f"    {repr(action)},")
            print("]")

            libstore = await find_libstore(args.rpc, keypair, args.giver)
            tx = await libstore.send(args.lib_actions)
            await track_tx(tx)

        case _:
            raise RuntimeError(f"Unknown subcommand {args.subparser_name}")


async def find_libstore(
    endpoint: str, keypair: nt.KeyPair, giver: Optional[nt.Address]
) -> LibStore:
    transport = nt.JrpcTransport(endpoint=args.rpc)
    await transport.check_connection()
    libstore = LibStore(transport, keypair)

    account = await transport.get_account_state(libstore.address)
    if account is None:
        if giver is not None:
            await topup_account(
                transport, keypair, giver, libstore.address, nt.Tokens(10)
            )
        else:
            raise RuntimeError(f"LibStore account does not exist: {libstore.address}")

    return libstore


async def topup_account(
    transport: nt.Transport,
    keypair: nt.KeyPair,
    giver_address: nt.Address,
    address: nt.Address,
    amount: nt.Tokens,
):
    default_giver = nt.contracts.EverWallet.from_address(
        transport,
        keypair,
        giver_address,
    )
    default_giver_state = await transport.get_account_state(giver_address)
    if default_giver_state is None:
        raise RuntimeError(f"Default giver does not exist: {giver_address}")

    tx = await default_giver.send(address, amount, payload=nt.Cell(), bounce=False)
    await transport.trace_transaction(tx).wait()


async def track_tx(tx: nt.Transaction):
    print(f"ext_in_msg_hash: {tx.in_msg_hash.hex()}")
    print(f"src_tx_hash: {tx.hash.hex()}")


def load_keys(sign: str):
    if len(sign) == 64:
        try:
            return nt.KeyPair(secret=bytes.fromhex())
        except ValueError:
            pass

    with open(os.path.join(dirname, sign)) as f:
        keys = json.load(f)
        secret = keys["secret"]
        if not isinstance(secret, str):
            raise RuntimeError("invalid keys")
        return nt.KeyPair(secret=bytes.fromhex(secret))


if __name__ == "__main__":
    asyncio.run(main())
