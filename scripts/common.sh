function is_number {
    if [ -n "$1" ] && [ "$1" -eq "$1" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
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
        if [ ! -z "$prev_version" ]; then
            echo "WARN: Clang $prev_version not found, fallback to Clang $clang_version."
        fi

        local cc_path=$(command -v "clang-$clang_version" 2>&1 || true)
        local cxx_path=$(command -v "clang++-$clang_version" 2>&1 || true)

        if [ ! -z "$cc_path" ] && [ ! -z "$cxx_path" ]; then
            export CC="$cc_path"
            export CXX="$cxx_path"
            return 0
        else
            prev_version="$clang_version"
        fi
    done

    if [ -z "$require" ]; then
        echo "WARN: No Clang versions found, fallback to default build."
        return 1
    else
        echo "ERROR: Clang is required but not found."
        exit 1
    fi
}
