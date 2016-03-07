#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
  --class com.eugen.streaming.SparkStreamingHadoop \
  snapshot/spark-streaming-hadoop-1.0-SNAPSHOT.jar \
    --zookeeper-quorum 0.0.0.0 \
    --consumer-group my-consumer-group \
    --master local[2] \
    --topics test \
    --threads 2 \
    --output-file lines-out.txt
