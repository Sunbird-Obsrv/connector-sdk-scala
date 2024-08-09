package org.sunbird.obsrv.connector.source

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.function.BaseProcessFunction
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.{JSONUtil, Metrics}

abstract class SourceConnectorFunction(connectorContexts: List[ConnectorContext]) extends BaseProcessFunction[String, String] {

  private def successFunction(event: String)(implicit ctx: ProcessFunction[String, String]#Context): Unit = {
    ctx.output(ConnectorConstants.CONNECTOR_SUCCESS_TAG, event)
  }

  private def failedFunction(event: String, error: ErrorData)(implicit ctx: ProcessFunction[String, String]#Context): Unit = {
    ctx.output(ConnectorConstants.CONNECTOR_FAILED_TAG, JSONUtil.serialize(Map("event" -> event, "error" -> error)))
  }

  private def incMetric(metric: String, count: Long)(implicit metrics: Metrics) : Unit = {
    if(getMetrics().contains(metric)) {
      connectorContexts.foreach(ctx => {
        metrics.incCounter(ctx.connectorInstanceId, metric, count)
      })
    }
  }

  override def processElement(event: String, context: ProcessFunction[String, String]#Context, metrics: Metrics): Unit = {

    connectorContexts.foreach(ctx => {
      super.initMetrics(ctx.connectorId, ctx.connectorInstanceId)
    })
    implicit val ctx: ProcessFunction[String, String]#Context = context
    implicit val mtx: Metrics = metrics
    processEvent(event, successFunction, failedFunction, incMetric)
  }

  def processEvent(event: String, onSuccess: String => Unit, onFailure: (String, ErrorData) => Unit, incMetric: (String, Long) => Unit): Unit

}
