package com.eugen.streaming

/**
  * Created by eugene on 3/7/16.
  */
case class Config(
                   zookeeperQuorum : String = "",
                   consumerGroup   : String = "",
                   master          : String = "",
                   topics          : String = "",
                   numThreads      : String = "",
                   outputFile      : String = ""
                 )