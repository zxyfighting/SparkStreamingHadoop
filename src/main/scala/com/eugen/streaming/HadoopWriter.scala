package com.eugen.streaming

import java.io.BufferedWriter
import java.io.OutputStreamWriter

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

class HadoopWriter(outputFile: String) {
  /**
   * Writes data to a file on a Hadoop filesystem.
   *
   * @param data Data to write to a file.
   */
  def save(data: String): Unit = {
    val dataln = data + "\n"

    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val hdfsOutputFilePath = new Path(outputFile)
    val encoding = "UTF-8"
    val bufferSize = 8192

    def outputStream = {
      if (hdfs.exists(hdfsOutputFilePath))
        hdfs.append(hdfsOutputFilePath, bufferSize, new ProgressWriter)
      else
        hdfs.create(hdfsOutputFilePath, new ProgressWriter)
    }

    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, encoding))

    writer.write(dataln)
    writer.close()

    hdfs.close()
  }
}
