package com.eugen.streaming

import java.io.BufferedWriter
import java.io.OutputStreamWriter

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

class HadoopWriter(outputFile: String) {
  private val conf = new Configuration()
  private val hdfs = FileSystem.get(conf)
  private val hdfsOutputFilePath = new Path(outputFile)
  private val encoding = "UTF-8"
  private val bufferSize = 8192

  /**
   * Writes data to a file on a Hadoop filesystem.
   *
   * @param data Data to write to a file.
   */
  def save(data: String): Unit = {
    val dataln = data + "\n"

    if (hdfs.exists(hdfsOutputFilePath)) append else write

    def append(): Unit = {
      val os = hdfs.append(hdfsOutputFilePath, bufferSize, new ProgressWriter)
      val writer = new BufferedWriter(new OutputStreamWriter(os, encoding))

      writer.write(dataln)
      writer.close()
    }

    def write(): Unit = {
      val os = hdfs.create(hdfsOutputFilePath, new ProgressWriter)
      val writer = new BufferedWriter(new OutputStreamWriter(os, encoding))

      writer.write(dataln)
      writer.close()
    }
  }

  /**
   * Closes filesystem.
   */
  def closeFileSystem(): Unit = hdfs.close()
}
