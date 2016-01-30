package com.eugen.streaming

import org.apache.hadoop.util.Progressable

class ProgressWriter extends Progressable {
  override def progress: Unit = print(".")
}
