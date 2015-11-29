package com.ievgenp.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.log4j.Logger

object ReceiverBased {
  def main(args: Array[String]) {
    if (args.length < 5) System.exit(1)

    val Array(zkQuorum, group, master, topics, numThreads) = args
    val sparkConf = new SparkConf().setMaster(master).setAppName("ReceiverBasedStreamingApp")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val Log = Logger.getLogger(ReceiverBased.this.getClass().getSimpleName())

    Log.info("DEBUG info:" + zkQuorum)

    streamingContext.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(streamingContext, zkQuorum, group, topicMap).map(_._2)

    lines.print()

    sys.addShutdownHook {
      Log.info("Goodbye.")
      streamingContext.stop(true, true)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
