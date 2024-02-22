package org.sunbird.obsrv.connector.source

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.connector.model.Models.ErrorEvent
import org.sunbird.obsrv.job.function.BaseWindowProcessFunction
import org.sunbird.obsrv.job.util.{JSONUtil, Metrics}

import java.lang
import scala.collection.JavaConverters._

abstract class SourceConnectorWindowFunction[W <: Window] extends BaseWindowProcessFunction[String, String, String, W] {

  override def getMetrics(): List[String] = {
    List()
  }

  private def successFunction(events: List[String])(implicit ctx: ProcessWindowFunction[String, String, String, W]#Context): Unit = {
    events.foreach(event => {
      ctx.output(ConnectorConstants.CONNECTOR_SUCCESS_TAG, event)
    })
  }

  private def failedFunction(events: List[ErrorEvent])(implicit ctx: ProcessWindowFunction[String, String, String, W]#Context): Unit = {
    events.foreach(errorEvent => {
      ctx.output(ConnectorConstants.CONNECTOR_FAILED_TAG, JSONUtil.serialize(Map("event" -> errorEvent.event, "error" -> errorEvent.error)))
    })
  }

  override def process(key: String, context: ProcessWindowFunction[String, String, String, W]#Context, elements: lang.Iterable[String], metrics: Metrics): Unit = {

    implicit val ctx = context
    val eventsList = elements.asScala.toList
    processEvents(key, eventsList, successFunction, failedFunction)
  }

  def processEvents(key: String, events: List[String], onSuccess: List[String] => Unit, onFailure: List[ErrorEvent] => Unit): Unit

}
