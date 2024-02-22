package org.sunbird.obsrv.connector.source

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.job.function.BaseProcessFunction
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.{JSONUtil, Metrics}

abstract class SourceConnectorFunction extends BaseProcessFunction[String, String] {

  override def getMetrics(): List[String] = {
    List()
  }

  private def successFunction(event: String)(implicit ctx: ProcessFunction[String, String]#Context): Unit = {
    ctx.output(ConnectorConstants.CONNECTOR_SUCCESS_TAG, event)
  }

  private def failedFunction(event: String, error: ErrorData)(implicit ctx: ProcessFunction[String, String]#Context): Unit = {
    ctx.output(ConnectorConstants.CONNECTOR_FAILED_TAG, JSONUtil.serialize(Map("event" -> event, "error" -> error)))
  }

  override def processElement(event: String, context: ProcessFunction[String, String]#Context, metrics: Metrics): Unit = {

    implicit val ctx: ProcessFunction[String, String]#Context = context
    processEvent(event, successFunction, failedFunction)
  }

  def processEvent(event: String, onSuccess: String => Unit, onFailure: (String, ErrorData) => Unit): Unit

}
