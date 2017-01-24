/**
  * Allows you to receive scala.collection.immutable.Stream
  */

package com.felstar.scalaStreaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class InfiniteStreamReceiver[T](stream: Stream[T], delay:Int=0, storageLevel: StorageLevel) extends Receiver[T](storageLevel) {

  override def onStart(): Unit = {
    new Thread("InfiniteStreamReceiver"){
      override def run(): Unit = {
        stream.takeWhile{_=> Thread.sleep(delay);!isStopped}.foreach(store)
      }
      setDaemon(true)
    }.start()
  }

  override def onStop(): Unit = {}
}
