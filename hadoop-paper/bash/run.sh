#!/bin/bash

SOURCE_CODE=/home/hdfs/biantao/source

if [ ! -d $SOURCE_CODE ];then
    echo "$SOURCE_CODE directory not exist, create it"
    mkdir -p $SOURCE_CODE
else
    echo "$SOURCE_CODE directory already exist"
fi