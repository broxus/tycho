#!/usr/bin/env python3
import os
import sys

def should_collect_coverage(crate_name: str) -> bool:
    """
    Determines if coverage should be collected for a crate.

    Logic:
    - In integration tests: only collect for tycho-* crates
    - Otherwise: collect for everything except blacklisted crates
    """
    # Default blacklist
    ALWAYS_BLACKLISTED = {"25519", "tokio"}

    # Integration test mode
    if os.environ.get("INTEGRATION_TEST"):
        return "tycho" in crate_name

    # Normal mode
    return crate_name not in ALWAYS_BLACKLISTED

def get_crate_name(args):
    """Extract crate name from rustc arguments"""
    for i, arg in enumerate(args):
        if arg == "--crate-name":
            return args[i + 1]
    return ""

def main():
    # The first argument is the path to rustc, followed by its arguments
    args = sys.argv[1:]

    # Get crate name
    crate_name = get_crate_name(args)

    # Check if we should collect coverage
    if not should_collect_coverage(crate_name):
        try:
            # Remove coverage instrumentation flags
            instrument_coverage_index = args.index("instrument-coverage")
            del args[instrument_coverage_index]
            del args[instrument_coverage_index - 1]
        except ValueError:
            pass

    # Execute rustc with the potentially modified arguments
    os.execvp(args[0], args)

if __name__ == "__main__":
    main()
