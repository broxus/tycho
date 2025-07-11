#!/usr/bin/env bash
set -eE

function print_help {
  echo 'Usage: repack-heavy-archives.sh [OPTIONS]'
  echo ''
  echo 'Options:'
  echo '  --dir         Integration tests directory'
}

test_dir=""
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
      --dir)
        test_dir="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected test dir'
          echo ''
          print_help
          exit 1
        fi
      ;;
      *) # unknown option
        echo 'ERROR: Unknown option'
        echo ''
        print_help
        exit 1
      ;;
  esac
done

if [ -z "${test_dir}" ]; then
    print_help
    exit 1
fi

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
root_dir=$(cd "${script_dir}/../" && pwd -P)

cd "$root_dir"
cargo test -p tycho-core repack_heavy_archives --features test -- --ignored

cd "$test_dir"
for i in {1..3}; do
    sha256sum archive_${i}.bin > archive_${i}.bin.sha256
done
