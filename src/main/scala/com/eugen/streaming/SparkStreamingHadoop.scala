package com.eugen.streaming

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object SparkStreamingHadoop {
  private val log = Logger.getLogger(SparkStreamingHadoop.this.getClass().getSimpleName())
  private val intervalBetweenRDDs = Seconds(5)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("SparkStreamingHadoop", "1.0")

      opt[String]('z', "zookeeper-quorum") action { (x, c) =>
        c.copy(zookeeperQuorum = x) } required()
      opt[String]('c', "consumer-group") action { (x, c) =>
        c.copy(consumerGroup = x) } required()
      opt[String]('m', "master") action { (x, c) =>
        c.copy(master = x) } required()
      opt[String]('t', "topics") action { (x, c) =>
        c.copy(topics = x) } required()
      opt[String]('n', "threads") action { (x, c) =>
        c.copy(numThreads = x) } required()
      opt[String]('f', "output-file") action { (x, c) =>
        c.copy(outputFile = x) } required()

      help("help") text("print this text")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        val sparkConf = new SparkConf()
          .setMaster(config.master)
          .setAppName("SparkStreamingHadoop")

        val streamingContext = new StreamingContext(sparkConf, intervalBetweenRDDs)

        val topicMap = config
          .topics
          .split(",")
          .map((_, config.numThreads.toInt)).toMap

        val hadoopWriter = new HadoopWriter(config.outputFile)

        val lines = KafkaUtils
          .createStream(streamingContext, config.zookeeperQuorum, config.consumerGroup, topicMap)
          .map(_._2)

        lines.foreachRDD(rdd => rdd
          .collect()
          .foreach(line => hadoopWriter.writeLine(line)))

        sys.addShutdownHook(onShutdown(streamingContext, hadoopWriter))

        streamingContext.start()
        streamingContext.awaitTermination()

      case None => println("Specify run parameters.")
    }
  }

  private def onShutdown(sc: StreamingContext, hadoopWriter: HadoopWriter): Unit = {
    log.info("Closing filesystem and stopping Spark streaming context…")

    hadoopWriter.closeFileSystem
    sc.stop(true, true)
  }
}
