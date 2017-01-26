/**
  * Simple core spark standalone example
  */

package com.felstar.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Spark1 extends App{

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val logFile = "README.md"
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //val logData = sc.textFile(logFile, 2).cache()

  val rddString=sc.parallelize(Array("a b","b a", "b b", "c d")).cache()

  def filterCount[T](rdd:RDD[T])(predicate: T=>Boolean):Long=rdd.filter(predicate).count()

  val rddStringFilterCount=filterCount(rddString) _

  val numAs = rddStringFilterCount(_.contains("a"))
  val numBs = rddStringFilterCount(_.contains("b"))

  println(s"Lines with a: $numAs, Lines with b: $numBs")

  val totalLength=rddString.map(_.length).sum().toLong
  println(s"Total length $totalLength")

  sc.stop()
}
