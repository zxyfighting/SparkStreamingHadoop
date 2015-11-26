#!/usr/bin/env bash
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --class com.ievgenp.streaming.ReceiverBased \
  target/basic-streaming-1.0-SNAPSHOT.jar zoo1 my-consumer-group test 1
