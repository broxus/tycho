# NOTE: Should only be used for file modifications.
# Example:
#   jq_inplace <file> <jqargs>...
function jq_inplace {
    local file="$1"
    shift 1
    local data
    data=$(cat "$file" | jq -e "$@")
    if [ $? -eq 0 ]; then
        echo -E "$data" > "$file"
    fi
}

function is_number {
    if [ -n "$1" ] && [ "$1" -eq "$1" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

function rustc_llvm_version {
    local full_version=$(rustc --version --verbose | grep LLVM | cut -d ' ' -f 3)
    local major_version=$(echo "$full_version" | cut -d '.' -f 1)
    echo "$major_version"
    return 0
}

function set_clang_env {
    local clang_versions=()
    local require=""
    while [[ $# -gt 0 ]]; do
        key="$1"
        case $key in
          require)
            require="true"
            shift # past argument
          ;;
          auto)
            clang_versions+=("$key")
            shift # past argument
          ;;
          ''|*[!0-9]*) # unknown option
            echo "ERROR: Unknown option \`$key\`"
            exit 1
          ;;
          *)
            clang_versions+=("$key")
            shift # past argument
          ;;
        esac
    done

    local prev_version=""
    for clang_version in "${clang_versions[@]}"; do
        if [ "$clang_version" == "auto" ]; then
            clang_version=$(rustc_llvm_version)
            # echo "INFO: \`rustc\` will use LLVM ${clang_version}"
        fi

        if [ ! -z "$prev_version" ]; then
            echo "WARN: Clang $prev_version not found, fallback to Clang $clang_version." >&2
        fi

        local cc_path=$(command -v "clang-$clang_version" 2>&1 || true)
        local cxx_path=$(command -v "clang++-$clang_version" 2>&1 || true)

        local lld_bin=""
        if command -v "ld.lld-$clang_version" 2>&1 >/dev/null; then
            lld_bin="lld-$clang_version"
        elif command -v "ld.lld" 2>&1 >/dev/null; then
            local lld_version=$(ld.lld -V | awk '{print $2}' | cut -d '.' -f 1)
            if [ "$lld_version" == "$clang_version" ]; then
                lld_bin="lld"
            fi
        fi

        if [ ! -z "$cc_path" ] && [ ! -z "$cxx_path" ] && [ ! -z "$lld_bin" ]; then
            export CC="$cc_path"
            export CXX="$cxx_path"
            export LD="$lld_bin"
            return 0
        else
            prev_version="$clang_version"
        fi
    done

    if [ -z "$require" ]; then
        echo "WARN: No Clang versions found, fallback to default build." >&2
        return 1
    else
        echo "ERROR: Clang is required but not found."
        exit 1
    fi
}
