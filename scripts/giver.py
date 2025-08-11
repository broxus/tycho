#!/usr/bin/env python3
import os
import asyncio
import argparse
import json
import nekoton as nt

dirname = os.path.dirname(__file__)

parser = argparse.ArgumentParser(description="Local Giver")
parser.add_argument("address", help="Destination account address", type=nt.Address)
parser.add_argument("amount", help="Amount of tokens to send", type=nt.Tokens)
parser.add_argument(
    "--rpc", required=False, help="JRPC endpoint URL", default="http://127.0.0.1:8001"
)
parser.add_argument(
    "--src",
    required=False,
    help="Overwrite giver address",
    type=nt.Address,
    default=nt.Address(
        "-1:1111111111111111111111111111111111111111111111111111111111111111"
    ),
)
parser.add_argument(
    "--sign",
    required=False,
    help="Path to the giver keys",
    default=os.path.join(dirname, "../keys.json"),
)
args = parser.parse_args()


async def main():
    transport = nt.JrpcTransport(endpoint=args.rpc)
    await transport.check_connection()

    print(f"from: {args.src}")
    print(f"to: {args.address}")
    print(f"amount: {args.amount}")

    keypair = load_keys(args.sign)
    giver = nt.contracts.EverWallet.from_address(transport, keypair, args.src)
    giver_balance = await giver.get_balance()
    print(f"giver_balance: {giver_balance}")

    print("sending message...")
    tx = await giver.send(args.address, args.amount, payload=nt.Cell(), bounce=False)
    print(f"ext_in_msg_hash: {tx.in_msg_hash.hex()}")
    print(f"src_tx_hash: {tx.hash.hex()}")

    async for dst_tx in transport.trace_transaction(tx):
        print(f"dst_tx_hash: {dst_tx.hash.hex()}")
        break

    dst_state = await transport.get_account_state(args.address)
    dst_balance = nt.Tokens(0)
    if dst_state is not None:
        dst_balance = dst_state.balance
    print(f"dst_balance: {dst_balance}")


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
