package com.eugen.streaming

import org.apache.log4j.Logger

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object ReceiverBased {
  private val log = Logger.getLogger(ReceiverBased.this.getClass().getSimpleName())

  def main(args: Array[String]) {
    if (args.length < 6) System.exit(1)

    val Array(zkQuorum, group, master, topics, numThreads, outputFile) = args
    val sparkConf = new SparkConf().setMaster(master).setAppName("ReceiverBasedStreamingApp")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val lines = KafkaUtils.createStream(streamingContext, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD(s => s.collect().foreach(new HadoopWriter(outputFile).save(_)))

    log.info("DEBUG info:" + zkQuorum)

    sys.addShutdownHook(onShutdown(streamingContext))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def onShutdown(sc: StreamingContext): Unit = {
    log.info("Stopping streaming context...")
    sc.stop(true, true)
    log.info("Done. Goodbye.")
  }
}
