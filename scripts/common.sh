function is_number {
    if [ -n "$1" ] && [ "$1" -eq "$1" ] 2>/dev/null; then
        return 0
    else
        return 1
    fi
}
