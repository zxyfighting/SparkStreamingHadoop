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
   * Determines whether the file with given path exists or not.
   */
  def fileExists: Boolean = hdfs.exists(hdfsOutputFilePath)

  /**
   * Writes a line specified to a file on a Hadoop filesystem.
   *
   * @param data Data to write to a file.
   */
  def writeLine(data: String): Unit = {
    def saveFSDataOutputStream(fsDataOutputStream: FSDataOutputStream): Unit = {
      val outputStreamWriter = new OutputStreamWriter(fsDataOutputStream, encoding)
      val bufferedWriter = new BufferedWriter(outputStreamWriter)

      bufferedWriter.write(s"${ data }\n")
      bufferedWriter.close()

      outputStreamWriter.close()
      fsDataOutputStream.close()
    }

    def append: Unit = {
      val fsDataOutputStream = hdfs.append(hdfsOutputFilePath)

      saveFSDataOutputStream(fsDataOutputStream)
    }

    def create: Unit = {
      val fsDataOutputStream = hdfs.create(hdfsOutputFilePath)

      saveFSDataOutputStream(fsDataOutputStream)
    }

    if (fileExists) append else create
  }

  /**
   * Closes filesystem.
   */
  def closeFileSystem: Unit = hdfs.close()
}
