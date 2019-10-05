#!/bin/bash

# create the input directory on HDFS
hadoop fs -mkdir -p input
# put the input files to all the datanodes on HDFS
hadoop fs -put /input/* input
#  run the jar program from inside namenode
hadoop jar /countWordHadoop.jar com.boubou.WordCount -Dwordcount.case.sensitive="${1:-false}" -Dwordcount.nb.results="${2:-100}" input output
# print out the program result
hadoop fs -head output/out2/part-r-00000
