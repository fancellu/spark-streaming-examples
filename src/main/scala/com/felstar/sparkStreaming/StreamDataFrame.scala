/**
  * Streaming + Spark SQL DataFrames
  */

package com.felstar.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object StreamDataFrame extends App{

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamDataFrame")
  val ssc = new StreamingContext(conf, Seconds(2))

  val stringRDD=ssc.sparkContext.parallelize(List("the cat", "sat on", "the mat"))

  val queue=mutable.Queue(stringRDD)

  val words = ssc.queueStream(queue).flatMap(_.split(" "))
  words.foreachRDD { rdd =>

    val sparkSession = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import sparkSession.implicits._

    val wordsDataFrame = rdd.toDF("word")

    wordsDataFrame.createOrReplaceTempView("words")

    val wordCountsDataFrame =
      sparkSession.sql("select word, count(*) as total from words group by word")

    if (!wordCountsDataFrame.rdd.isEmpty) wordCountsDataFrame.show()
  }

  ssc.start()

    // wait 2 seconds, then add more stuff to the existing queue
  Thread.sleep(2000)
  val stringRDD2=ssc.sparkContext.parallelize(List("the cat", "sat on", "theeeeee mat"))
  queue.enqueue(stringRDD2)

  ssc.awaitTermination()
}
