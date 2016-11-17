#!/bin/bash

SRCDIR=$( cd -P $(dirname ${BASH_SOURCE[0]}); echo $PWD )

spark-submit \
    --master local[*] \
    --class com.drakeconsulting.big_data_maker.SimpleApp \
    $SRCDIR/target/scala-2.10/BigDataMaker-assembly-1.0.jar $*
