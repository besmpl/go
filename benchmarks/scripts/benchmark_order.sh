#!/usr/bin/env bash
# Source-safe scheduling helper for the three-backend comparison harness.

comparison_order() {
    local sample remainder digit index
    if (( $# != 1 )) || [[ ! "$1" =~ ^[1-9][0-9]*$ ]]; then
        echo "comparison_order requires one positive integer sample number" >&2
        return 1
    fi

    # Reduce the decimal string one digit at a time so validation remains
    # correct even when SAMPLE is larger than Bash's native integer range.
    sample="$1"
    remainder=0
    for (( index = 0; index < ${#sample}; index++ )); do
        digit="${sample:index:1}"
        remainder=$(( (remainder * 10 + digit) % 6 ))
    done
    case $(( (remainder + 5) % 6 )) in
        0) echo 'baseline tsan purego' ;;
        1) echo 'tsan purego baseline' ;;
        2) echo 'purego baseline tsan' ;;
        3) echo 'baseline purego tsan' ;;
        4) echo 'purego tsan baseline' ;;
        5) echo 'tsan baseline purego' ;;
    esac
}
