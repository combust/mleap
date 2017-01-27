#!/usr/bin/env bash

# The current directory of the script.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" = 0 ]; then
    ARGS="--nologcapture --exclude-dir=mleap/pyspark --verbose"
else
    ARGS="$@"
fi
exec nosetests $ARGS --where $DIR