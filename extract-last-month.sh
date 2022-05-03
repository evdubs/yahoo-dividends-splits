#!/usr/bin/env bash

month_ago=$(date -d "-1 month" "+%F")
dir=$(dirname "$0")

racket -y ${dir}/extract.rkt -c "$1" -p "$2" -r "$3" -s ${month_ago}
