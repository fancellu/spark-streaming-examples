/**
  * Example usage of stream state
  */

package com.felstar.scalaStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object StreamState extends App{

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamState")
  val ssc = new StreamingContext(conf, Seconds(3))

  val tempDir = System.getProperty("java.io.tmpdir")

  ssc.checkpoint(tempDir)

  val stringRDD=ssc.sparkContext.parallelize(List("the cat", "sat on", "the mat"))

  val queue=mutable.Queue(stringRDD)

  {
    // note the use of queueStream to inject a mutable.Queue
    // useful for standalone apps, testing etc.
    val lines=ssc.queueStream(queue)

    val updateFunction= (newValues: Seq[Long], runningCount: Option[Long]) =>
      Some(newValues.sum + runningCount.getOrElse(0L))

    val words = lines.flatMap(_.split(" "))

    val wordCounts = words.countByValue()
    val runningCounts = wordCounts.updateStateByKey[Long](updateFunction)
    runningCounts.print()
  }

  ssc.start()

    // wait 2 seconds, then add more stuff to the existing queue
  Thread.sleep(2000)
  val stringRDD2=ssc.sparkContext.parallelize(List("the cat", "sat on", "theeeeee mat"))
  queue.enqueue(stringRDD2)

  ssc.awaitTermination()
}
