#!/usr/bin/env python3
import os
import asyncio
import argparse
import json
import time
import nekoton as nt
from typing import List

dirname = os.path.dirname(__file__)


class AddressHash:
    def __init__(self, addr: str):
        if len(addr) != 64:
            raise ValueError("invalid address hash")
        self.hash = bytes.fromhex(addr)


parser = argparse.ArgumentParser(description="PoA manager")
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

subparsers = parser.add_subparsers(required=True, dest="command")

cmd_address = subparsers.add_parser(
    "address", help="Get libstore address", parents=[common_parser]
)

cmd_add = subparsers.add_parser(
    "add", help="Add whitelisted account", parents=[common_parser]
)
cmd_add.add_argument("address", help="Validator address", type=AddressHash)

cmd_remove = subparsers.add_parser(
    "remove", help="Remove whitelisted account", parents=[common_parser]
)
cmd_remove.add_argument("address", help="Validator address", type=AddressHash)

args = parser.parse_args()

ELECTOR_POA_OP_ADD_ADDRESS = 0x206491DE
ELECTOR_POA_OP_REMOVE_ADDRESS = 0x56EFD52D
ANSWER_TAG_POA_WHITELIST_UPDATED = 0xBC06677E
ANSWER_TAG_ERROR = 0xFFFFFFFF

MIN_BALANCE = nt.Tokens(2)
ATTACHED_AMOUNT = nt.Tokens(1)


async def main():
    transport = nt.JrpcTransport(endpoint=args.rpc)
    await transport.check_connection()

    config = await transport.get_blockchain_config()
    assigned_manager_cs = config.get_raw_param(101)
    if assigned_manager_cs is None:
        raise RuntimeError("no PoA manager set")
    assigned_manager = nt.Address.from_parts(
        -1, assigned_manager_cs.as_slice().load_bytes(32)
    )

    match args.command:
        case "address":
            print(assigned_manager)
            return
        case "add":
            op = ELECTOR_POA_OP_ADD_ADDRESS
        case "remove":
            op = ELECTOR_POA_OP_REMOVE_ADDRESS
        case _:
            raise RuntimeError(f"Unknown subcommand {args.command}")

    keypair = load_keys(args.sign)
    manager = nt.contracts.EverWallet.from_address(transport, keypair, assigned_manager)
    print(f"manager_address: {manager.address}")

    manager_balance = await manager.get_balance()
    print(f"giver_balance: {manager_balance}")
    if manager_balance < MIN_BALANCE:
        raise RuntimeError("manager balance is too low")

    query_id = round(time.time() * 1000)
    body = nt.CellBuilder()
    body.store_u32(op)
    body.store_u64(query_id)
    body.store_bytes(args.address.hash)

    print("sending message...")
    tx = await manager.send(
        dst=config.elector_address,
        value=ATTACHED_AMOUNT,
        payload=body.build(),
        bounce=True,
    )
    print(f"ext_in_msg_hash: {tx.in_msg_hash.hex()}")
    print(f"src_tx_hash: {tx.hash.hex()}")
    async for dst_tx in transport.trace_transaction(tx):
        if dst_tx.account == config.elector_address.account:
            print(f"dst_tx_hash: {dst_tx.hash.hex()}")

            callbacks: List[nt.Message] = dst_tx.get_out_msgs()
            for callback in callbacks:
                if callback.dst != manager.address:
                    continue

                cs = callback.body.as_slice()
                answer_tag = cs.load_u32()
                if answer_tag == ANSWER_TAG_POA_WHITELIST_UPDATED:
                    print("whitelist updated")
                elif answer_tag == ANSWER_TAG_ERROR:
                    print("failed to update whitelist")
                else:
                    print(f"unknown answer tag: 0x{answer_tag:08x}")
                return

    raise RuntimeError("no destination transaction found")


def load_keys(sign: str):
    if len(sign) == 64:
        try:
            return nt.KeyPair(secret=bytes.fromhex(sign))
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
