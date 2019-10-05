#!/bin/bash

# to remove :WARNING: HADOOP_PREFIX has been replaced by HADOOP_HOME. Using value of HADOOP_PREFIX.
export HADOOP_HOME=$HADOOP_PREFIX
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
unset HADOOP_PREFIX

# create the input directory on HDFS
hadoop fs -mkdir -p input
# put the input files to all the datanodes on HDFS
hadoop fs -put /input/* input
#  run the jar program from inside namenode
hadoop jar /countWordHadoop.jar com.boubou.WordCount -Dwordcount.case.sensitive="${1:-false}" -Dwordcount.nb.results="${2:-100}" input output
# print out the program result
hadoop fs -head output/out2/part-r-00000
