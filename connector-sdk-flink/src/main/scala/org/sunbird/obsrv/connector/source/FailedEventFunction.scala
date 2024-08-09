package org.sunbird.obsrv.connector.source

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.function.BaseProcessFunction
import org.sunbird.obsrv.job.model.Models.{ContextData, ErrorData}
import org.sunbird.obsrv.job.model.StatusCode
import org.sunbird.obsrv.job.util.{JSONUtil, Metrics}

import scala.collection.mutable

class FailedEventFunction(connectorCtx: ConnectorContext) extends BaseProcessFunction[String, String] {
  override def processElement(event: String, context: ProcessFunction[String, String]#Context, metrics: Metrics): Unit =  {

    super.initMetrics(connectorCtx.connectorId, connectorCtx.connectorInstanceId)
    metrics.incCounter(connectorCtx.connectorInstanceId, ConnectorConstants.CONNECTOR_FAILED_COUNT)
    val map = JSONUtil.deserialize[mutable.Map[String, AnyRef]](event)

    context.output(ConnectorConstants.FAILED_OUTPUT_TAG, toObsrvEvent(map))
    context.output(ConnectorConstants.SYSTEM_EVENTS_TAG, toSystemEvent(map))
  }

  override def getMetrics(): List[String] = {
    List(ConnectorConstants.CONNECTOR_FAILED_COUNT)
  }

  private def toObsrvEvent(map: mutable.Map[String, AnyRef]): String = {
    map.put(ConnectorConstants.CONNECTOR_CTX, connectorCtx)
    JSONUtil.serialize(map)
  }

  private def toSystemEvent(map: mutable.Map[String, AnyRef]): String = {
    val error = JSONUtil.deserialize[ErrorData](JSONUtil.serialize(map.get("error")))
    val sysEvent = generateSystemEvent(
      contextData = ContextData(connectorId = connectorCtx.connectorId, datasetId = connectorCtx.datasetId, connectorInstanceId = connectorCtx.connectorInstanceId,
        connectorType = connectorCtx.connectorType, dataFormat = connectorCtx.dataFormat),
      error = error, StatusCode.failed
    )
    JSONUtil.serialize(sysEvent)
  }
}