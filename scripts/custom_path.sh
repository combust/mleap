#! /bin/sh

get_filtered_path() {
    literal=$1

    echo "$PATH" | tr ':' '\n' | grep -vF "$literal" | tr '\n' ':'
}
