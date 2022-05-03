#!/usr/bin/env bash

yesterday=$(date -d "-1 day" "+%F")
dir=$(dirname "$0")

racket -y ${dir}/transform-load.rkt -d ${yesterday} -p "$1"

7zr a /var/tmp/yahoo/dividends-splits/${yesterday}.7z /var/tmp/yahoo/dividends-splits/${yesterday}/*.csv

racket -y ${dir}/dump-dolt-dividends.rkt -p "$1"
