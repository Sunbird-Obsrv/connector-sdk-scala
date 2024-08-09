package org.sunbird.obsrv.connector

import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.util.Collections

class SuccessSink extends SinkFunction[String] {
  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    EventsSink.successEvents.add(value)
  }
}

object EventsSink {
  val successEvents: java.util.List[String] = Collections.synchronizedList(new java.util.ArrayList[String]())
  val failedEvents: java.util.List[String] = Collections.synchronizedList(new java.util.ArrayList[String]())
}

class FailedSink extends SinkFunction[String] {
  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    EventsSink.failedEvents.add(value)
  }
}
