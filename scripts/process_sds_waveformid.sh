#!/usr/bin/env bash

export LIBMSEED_LEAPSECOND_FILE="/home/marc/arclink-mass-downloader.old/leap-seconds.list"

RAW_DIR="/repacked/AlpArray/RAW"
SDS_DIR="/repacked/AlpArray/SDS"

make_sds() {
        # args: net, sta, loc, chan
        net=$1
        sta=$2
        loc=$3
        chan=$4

        echo  $net.$sta.$loc.$chan
        files=$(find $RAW_DIR -size +4096c -name "*$net.$sta.$loc.$chan*" | sort | xargs)
        dataselect-v3.21 -Ps -Q M -SDS $SDS_DIR  -v -sum $files
}

make_sds "$1" "$2" "$3" "$4"
