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

# States
state_checksum_file="${test_dir}/states.tar.zst.sha256"
state_checksum_url="${test_base_url}/states.tar.zst.sha256"

state_file="${test_dir}/states.tar.zst"
state_url="${test_base_url}/states.tar.zst"

# Zerostate
zerostate_checksum_file="${test_dir}/zerostate.boc.sha256"
zerostate_checksum_url="${test_base_url}/zerostate.boc.sha256"

zerostate_file="${test_dir}/zerostate.boc"
zerostate_url="${test_base_url}/zerostate.boc"

# Archives
archive_checksum_file="${test_dir}/archive.bin.sha256"
archive_checksum_url="${test_base_url}/archive.bin.sha256"

next_archive_checksum_file="${test_dir}/next_archive.bin.sha256"
next_archive_checksum_url="${test_base_url}/next_archive.bin.sha256"

last_archive_checksum_file="${test_dir}/last_archive.bin.sha256"
last_archive_checksum_url="${test_base_url}/last_archive.bin.sha256"

archive_file="${test_dir}/archive.bin"
archive_url="${test_base_url}/archive.bin"

next_archive_file="${test_dir}/next_archive.bin"
next_archive_url="${test_base_url}/next_archive.bin"

last_archive_file="${test_dir}/last_archive.bin"
last_archive_url="${test_base_url}/last_archive.bin"

# Always download the checksum file first to ensure it's the latest
echo "Downloading checksum file..."
curl --request GET -sL --url "${state_checksum_url}" --output "${state_checksum_file}"
curl --request GET -sL --url "${zerostate_checksum_url}" --output "${zerostate_checksum_file}"
curl --request GET -sL --url "${archive_checksum_url}" --output "${archive_checksum_file}"
curl --request GET -sL --url "${next_archive_checksum_url}" --output "${next_archive_checksum_file}"
curl --request GET -sL --url "${last_archive_checksum_url}" --output "${last_archive_checksum_file}"

function download() {
    curl --request GET -L --url $1 --output $2
}

# Check if the states file exists
if [ -f "${test_dir}/states.tar.zst" ]; then
    # Verify the archive against the checksum
    echo "Verifying existing states against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/states.tar.zst.sha256"; then
        echo "Checksum matches. No need to download the states."
    else
        echo "Checksum does not match. Downloading the states..."
        download ${state_url} ${state_file}
    fi
else
    echo "States file does not exist. Downloading the states..."
    download ${state_url} ${state_file}
fi

# Check if the zerostate file exists
if [ -f "${test_dir}/zerostate.boc" ]; then
    # Verify the zerostate against the checksum
    echo "Verifying existing zerostate against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/zerostate.boc.sha256"; then
        echo "Checksum matches. No need to download the zerostate."
    else
        echo "Checksum does not match. Downloading the zerostate..."
        download ${zerostate_url} ${zerostate_file}
    fi
else
    echo "Zerostate file does not exist. Downloading the zerostate..."
    download ${zerostate_url} ${zerostate_file}
fi

# Check if the archive file exists
if [ -f "${test_dir}/archive.bin" ]; then
    # Verify the archive against the checksum
    echo "Verifying existing archive against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/archive.bin.sha256"; then
        echo "Checksum matches. No need to download the archive."
    else
        echo "Checksum does not match. Downloading the archive..."
        download ${archive_url} ${archive_file}
    fi
else
    echo "Archive file does not exist. Downloading the archive..."
    download ${archive_url} ${archive_file}
fi

# Check if the next archive file exists
if [ -f "${test_dir}/next_archive.bin" ]; then
    # Verify the next archive against the checksum
    echo "Verifying existing next archive against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/next_archive.bin.sha256"; then
        echo "Checksum matches. No need to download the next archive."
    else
        echo "Checksum does not match. Downloading the next archive..."
        download ${next_archive_url} ${next_archive_file}
    fi
else
    echo "Next archive file does not exist. Downloading the next archive..."
    download ${next_archive_url} ${next_archive_file}
fi

# Check if the last archive file exists
if [ -f "${test_dir}/last_archive.bin" ]; then
    # Verify the last archive against the checksum
    echo "Verifying existing last archive against checksum..."

    cd "${test_dir}"
    if sha256sum -c "${test_dir}/last_archive.bin.sha256"; then
        echo "Checksum matches. No need to download the last archive."
    else
        echo "Checksum does not match. Downloading the last archive..."
        download ${last_archive_url} ${last_archive_file}
    fi
else
    echo "Last archive file does not exist. Downloading the last archive..."
    download ${last_archive_url} ${last_archive_file}
fi
