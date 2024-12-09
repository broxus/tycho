function is_number {
    if [ -n "$1" ] && [ "$1" -eq "$1" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

function set_clang_env {
    local clang_version="$1"
    if [ -z "$clang_version" ]; then
        echo "ERROR: Expected clang versrion for 'set_clang_env'."
        exit 1
    fi

    if [ -z "$2" ] || [ "$2" == "require" ]; then
        local require="$2"
    else
        echo "ERROR: Expected optional 'require' flag for 'set_clang_env'."
        exit 1
    fi

    local cc_path=$(command -v "clang-${clang_version}" 2>&1 || true)
    local cxx_path=$(command -v "clang++-${clang_version}" 2>&1 || true)

    if [ ! -z "$cc_path" ] && [ ! -z "$cxx_path" ]; then
        export CC="$cc_path"
        export CXX="$cxx_path"
    elif [ -z "$require" ]; then
        echo "WARN: Clang ${clang_version} not found, fallback to default build."
    else
        echo "ERROR: Clang ${clang_version} required but not found."
        exit 1
    fi
}
