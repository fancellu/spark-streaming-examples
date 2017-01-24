/**
  * Example of ingesting Scala streams and using a custom receiver
  */

package com.felstar.scalaStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamInf extends App{

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // ensure you have enough cores allocated to handle the receivers
  // else you'll see no data coming through, and wonder why.
  val conf = new SparkConf().setMaster("local[3]").setAppName("StreamInf")
  val ssc = new StreamingContext(conf, Seconds(2))

  val stream=Stream.from(-100)
  val lines = ssc.receiverStream(new InfiniteStreamReceiver(stream, 220, StorageLevel.MEMORY_ONLY))

  val stream2=Stream.from(100)
  val lines2 = ssc.receiverStream(new InfiniteStreamReceiver(stream2, 200, StorageLevel.MEMORY_ONLY))

  val union=ssc.union(Seq(lines,lines2))

  union.print()

  ssc.start()

  ssc.awaitTermination()
}
