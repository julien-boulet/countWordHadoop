#!/bin/bash

hadoop fs -mkdir -p input
hadoop fs -put /input/* input
hadoop jar /countWordHadoop.jar com.boubou.WordCount -Dwordcount.case.sensitive=false input output
hadoop fs -head output/out2/part-r-00000
