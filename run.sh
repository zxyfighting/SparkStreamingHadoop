#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
  --class com.eugen.streaming.SparkStreamingHadoop \
  snapshot/spark-streaming-hadoop-1.0-SNAPSHOT.jar 0.0.0.0 my-consumer-group local[2] test 2 lines-out.txt
