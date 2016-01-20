#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
  --class com.ievgenp.streaming.ReceiverBased \
  snapshot/basic-streaming-1.0-SNAPSHOT.jar zoo1 my-consumer-group local[2] test 2
