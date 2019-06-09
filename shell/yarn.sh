#!/bin/bash

$HADOOP_HOME/sbin/start-all.sh
$HADOOP_HOME/sbin/start-yarn.sh

hadoop fs -mkdir /logs
hadoop fs -put /logs/access_log_Jul95 /logs/log

spark-submit --class com.nikas.Application --master yarn --deploy-mode client /jar/parser-1.0-SNAPSHOT.jar