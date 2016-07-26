#!/bin/bash

echo "清理输出目录"
hdfs dfs -test -d /user/biantao/a_out
if [ $? -eq 0 ];
then
    echo "/user/biantao/a_out exists, delete now"
    hdfs dfs -rm -r /user/biantao/a_out
fi

hadoop jar /Users/biantao/workspace/test_space/Hadoop-startup/hadoop-paper/target/hadoop-paper-1.0-SNAPSHOT.jar \
com.cstnet.cnnic.VideoDistributed \
-Dcnic.source=/user/biantao/a \
-Dcnic.sort=1 \
/user/biantao/a \
/user/biantao/a_out
