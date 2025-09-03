#!/usr/bin/env python3
import asyncio
import argparse
from datetime import datetime
import nekoton as nt

parser = argparse.ArgumentParser(description="Watch Elector")
parser.add_argument("direction", help="Direction of traversing", choices=["old", "new"])
parser.add_argument(
    "--rpc", required=False, help="JRPC endpoint URL", default="http://127.0.0.1:8001"
)
parser.add_argument("--all", help="Show all transactions", action="store_true")
parser.add_argument("--since", help="Unix timestamp of range lower bound", type=int)
parser.add_argument("--until", help="Unix timestamp of range upper bound", type=int)
args = parser.parse_args()

minter_address: nt.Address


async def main():
    global minter_address

    transport = nt.JrpcTransport(endpoint=args.rpc)
    await transport.check_connection()

    config = await transport.get_blockchain_config()
    elector_address = config.elector_address
    minter_address = config.minter_address

    print(f"Elector: {elector_address}")

    match args.direction:
        case "old":
            await process_old_transactions(transport, elector_address)
        case "new":
            await process_new_transactions(transport, elector_address)


async def process_old_transactions(
    transport: nt.Transport, elector_address: nt.Address
):
    limit = 50
    lt = 18446744073709551614  # u64::MAX - 1 (to ignore the public jrpc bug)
    while True:
        res = await transport.get_transactions(elector_address, lt, limit)
        for tx in res:
            lt = tx.prev_trans_lt

            if args.since is not None and tx.now < args.since:
                return
            if args.until is not None and tx.now > args.until:
                continue

            handle_tx(tx)

        if lt == 0 or len(res) < limit:
            return


async def process_new_transactions(
    transport: nt.Transport, elector_address: nt.Address
):
    async with transport.account_transactions(elector_address) as batches:
        async for batch, batch_info in batches:
            for tx in batch:
                if args.since is not None and tx.now < args.since:
                    continue
                if args.until is not None and tx.now > args.until:
                    return

                handle_tx(tx)


def handle_tx(tx: nt.Transaction):
    if tx.type.is_ordinary:
        handle_ordinary_tx(tx)
    elif args.all:
        handle_ticktock_tx(tx)


def handle_ordinary_tx(tx: nt.Transaction):
    in_msg = tx.get_in_msg()
    if not args.all and in_msg.src == minter_address:
        return

    print_header(tx)
    body = nt.Cell().as_slice() if in_msg.body is None else in_msg.body.as_slice()

    try:
        if body.is_empty() or body.bits < 32 or body.get_u32(0) == 0:
            print(
                f">>> simple transfer\n    from: {in_msg.src}\n    amount: {in_msg.value}"
            )
            return

        op = body.load_u32()
        _query_id = body.load_u64()

        match op:
            case 0x4E73744B:
                validator_pubkey = body.load_bytes(32)
                stake_at = body.load_u32()
                max_factor = body.load_u32()
                adnl_addr = body.load_bytes(32)
                signature = body.load_reference().as_slice().load_bytes(64)

                print(
                    f">>> new stake\n\
    from: {in_msg.src}\n\
    pubkey: {validator_pubkey.hex()}\n\
    stake_at: {datetime.fromtimestamp(stake_at)} ({stake_at})\n\
    max_factor: {max_factor}\n\
    adnl_addr: {adnl_addr.hex()}\n\
    signature: {signature.hex()}"
                )

            case 0x47657424:
                print(">>> recover stake")

            case 0x4E436F64:
                code = body.load_reference()
                print(f">>> upgrade code\n    code_hash: {code.repr_hash.hex()}")

            case 0xEE764F4B | 0xEE764F6F:
                print(">>> config set confirmed")

    except Exception as e:
        print(f">>> unknown query\n    from: {in_msg.src}\n    amount: {in_msg.value}")
        print(e)

    print_response(tx, in_msg.src)


def handle_ticktock_tx(tx: nt.Transaction):
    print_header(tx)


def print_header(tx: nt.Transaction):
    print(f"\n=== {tx.type}, LT: {tx.lt} ===")
    print(f"Hash: {tx.hash.hex()}")
    print(f"Time: {datetime.fromtimestamp(tx.now)} ({tx.now})")


def print_response(tx: nt.Transaction, src: nt.Address):
    out_msgs = tx.get_out_msgs()
    for out_msg in out_msgs:
        if out_msg.dst != src:
            continue
        if out_msg.body is None:
            continue
        body = out_msg.body.as_slice()
        if body.bits < 96:
            continue

        op = body.load_u32()
        _query_id = body.load_u64()
        reason = 0 if body.bits < 32 else body.load_u32()

        match op:
            case 0xEE6F454C:
                print(f"<<< return stake\n    reason: {return_stake_reason(reason)}")
            case 0xF374484C:
                print("<<< stake confirmed")
            case 0xF96F7324:
                print(f"<<< stake recovered\n    amount: {out_msg.value}")
            case 0xFFFFFFFE:
                print("<<< stake recover failed")
            case 0xCE436F64:
                print("<<< code successfully upgraded")
            case 0xFFFFFFFF:
                print("<<< operation failed")
            case _:
                print(f"<<< unknown op {hex(op)}")

        return


def return_stake_reason(reason: int) -> str:
    match reason:
        case 0:
            return "no elections active, or source is not in masterchain"
        case 1:
            return "incorrect signature"
        case 2:
            return "stake smaller than 1/4096 of the total accumulated stakes"
        case 3:
            return "stake for some other elections"
        case 4:
            return "can make stakes for a public key from one address only"
        case 5:
            return "stake too small, return it"
        case 6:
            return "factor must be >= 1. = 65536/65536"
        case _:
            return "unknown"


if __name__ == "__main__":
    asyncio.run(main())
