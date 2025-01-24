#!/usr/bin/env bash
set -eE

function print_help {
  echo 'Usage: prepare-integration-tests.sh [OPTIONS]'
  echo ''
  echo 'Options:'
  echo '  --dir         Integration tests directory'
  echo '  --base-url    Base url for downloading test data'
}

test_dir=""
test_base_url=""
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
      --base-url)
        test_base_url="$2"
        shift # past argument
        if [ "$#" -gt 0 ]; then shift;
        else
          echo 'ERROR: Expected base url'
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

if [ -z "${test_dir}" ] || [ -z "${test_base_url}" ]; then
    print_help
    exit 1
fi

# Create the integration test directory if it does not exist
echo "Integration test directory: ${test_dir}"
mkdir -p "${test_dir}"

function download() {
    curl --request GET -L --url $1 --output $2
}

function ensure_exists() {
    local file_url="${test_base_url}/$1"
    local file_path="${test_dir}/$2"
    local checksum_url="${test_base_url}/$1.sha256"
    local checksum_path="${test_dir}/$2.sha256"

    echo "[$1]: Downloading checksum file..."
    curl --request GET -sL --url "${checksum_url}" --output "${checksum_path}"

    # Check if file exists
    if [ -f "$file_path" ]; then
        # Verify it against the checksum
        echo "[$1]: Verifying $1 against checksum..."

        cd "${test_dir}"
        if sha256sum -c "$checksum_path"; then
            echo "[$1]: Checksum matches. No need to download..."
        else
            echo "[$1]: Checksum does not match. Downloading..."
            download ${file_url} ${file_path}
        fi
        cd -
    else
        echo "[$1]: File does not exist. Downloading it..."
        download ${file_url} ${file_path}
    fi

    echo "[$1]: Done."
}

ensure_exists "states.tar.zst" "states.tar.zst"
ensure_exists "2025-01-24_zerostate.boc" "zerostate.boc"

ensure_exists "2025-01-24_archive_1.bin" "archive_1.bin"
ensure_exists "2025-01-24_archive_2.bin" "archive_2.bin"
ensure_exists "2025-01-24_archive_3.bin" "archive_3.bin"
