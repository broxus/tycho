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

state_checksum_file="${test_dir}/states.tar.zst.sha256"
state_checksum_url="${test_base_url}/states.tar.zst.sha256"

state_file="${test_dir}/states.tar.zst"
state_url="${test_base_url}/states.tar.zst"

# Always download the checksum file first to ensure it's the latest
echo "Downloading checksum file..."
curl --request GET -sL --url "${state_checksum_url}" --output "${state_checksum_file}"

function download_archive {
    curl --request GET -L --url "${state_url}" --output "${state_file}"
}

# Check if the archive file exists
if [ -f "${test_dir}/states.tar.zst" ]; then
    # Verify the archive against the checksum
    echo "Verifying existing archive against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/states.tar.zst.sha256"; then
        echo "Checksum matches. No need to download the archive."
    else
        echo "Checksum does not match. Downloading the archive..."
        download_archive
    fi
else
    echo "Archive file does not exist. Downloading the archive..."
    download_archive
fi
