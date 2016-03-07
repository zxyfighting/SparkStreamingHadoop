package com.eugen.streaming

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object SparkStreamingHadoop {
  private val log = Logger.getLogger(SparkStreamingHadoop.this.getClass().getSimpleName())

  def main(args: Array[String]) {
    if (args.length < 6) System.exit(1)

    val Array(zkQuorum, group, master, topics, numThreads, outputFile) = args
    val sparkConf = new SparkConf().setMaster(master).setAppName("SparkStreamingHadoop")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val hadoopWriter = new HadoopWriter(outputFile)

    val lines = KafkaUtils.createStream(streamingContext, zkQuorum, group, topicMap).map(_._2)

    lines.foreachRDD(s => s.collect().foreach(hadoopWriter.writeLine(_)))

    log.info("DEBUG info:" + zkQuorum)

    sys.addShutdownHook(onShutdown(streamingContext, hadoopWriter))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  private def onShutdown(sc: StreamingContext, hadoopWriter: HadoopWriter): Unit = {
    log.info("Closing filesystem and stopping Spark streaming context…")

    hadoopWriter.closeFileSystem
    sc.stop(true, true)
  }
}
