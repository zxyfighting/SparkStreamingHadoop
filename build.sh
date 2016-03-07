#!/usr/bin/env bash
set -e
mvn clean package
mv target/spark-streaming-hadoop-1.0-SNAPSHOT.jar snapshot/
