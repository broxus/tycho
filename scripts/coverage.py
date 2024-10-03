#!/usr/bin/env python3
import os
import sys


blacklist = ["25519", "tokio"]


def main():
    # The first argument is the path to rustc, followed by its arguments
    args = sys.argv[1:]

    # coverage is necessary only for our project
    crate_name = get_crate_name(args)
    if any(crate in crate_name for crate in blacklist):
        try:
            instrument_coverage_index = args.index("instrument-coverage")
            del args[instrument_coverage_index]
            del args[instrument_coverage_index - 1]
        except ValueError:
            pass

    # Execute rustc with the potentially modified arguments
    os.execvp(args[0], args)


def get_crate_name(args):
    for i, arg in enumerate(args):
        if arg == "--crate-name":
            return args[i + 1]
    return ""


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


if __name__ == "__main__":
    main()
