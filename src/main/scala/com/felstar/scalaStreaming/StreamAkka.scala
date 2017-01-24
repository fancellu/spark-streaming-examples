/**
  * Example of getting a stream of data from an Akka actor
  */

package com.felstar.scalaStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import akka.actor.{ActorSystem, Props}
import org.apache.spark.streaming.akka.{ActorReceiver, AkkaUtils}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

case object Fire

class MyActorReceiver[T] extends ActorReceiver {

  val words=Array("the","cat","sat","on","mat")

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, Fire.getClass)
  }
    // we choose randomly from array. store is where we emit to spark stream
  def receive: PartialFunction[Any, Unit] = {
    case Fire => store(words(Random.nextInt(words.size)))
  }

  override def postStop(): Unit = {
    context.system.eventStream.unsubscribe(self)
  }
}

object StreamAkka extends App{

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[2]").setAppName("StreamAkka")
  val ssc = new StreamingContext(conf, Seconds(3))

  val system = ActorSystem("mySystem")

    // note I specific existing actor system to use, else will create its own and hence eventbus will be split
  val lines = AkkaUtils.createStream[String]( ssc,Props(classOf[MyActorReceiver[String]]),"MyReceiver", actorSystemCreator = {
    ()=>system
  })

    // we send Fire object to event bus, MyActorReceiver is already subscribed
  val everyHalfSecond = system.scheduler.schedule(500.milliseconds, 500.milliseconds){
    system.eventStream.publish(Fire)
  }

  val counts=lines.countByValue()
   // emits counts of each word seen in last 3 seconds
  counts.print()

  ssc.start()

  ssc.awaitTermination()
}
