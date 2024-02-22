package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.exception.ObsrvException
import org.sunbird.obsrv.job.function.BaseProcessFunction
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.{JSONUtil, Metrics}

class SuccessEventFunction(connectorCtx: ConnectorContext, config: Config) extends BaseProcessFunction[String, String] {

  private val maxEventSize = config.getInt("kafka.producer.max-request-size")

  override def getMetrics(): List[String] = {
    List(ConnectorConstants.CONNECTOR_SUCCESS_COUNT, ConnectorConstants.OBSRV_FAILED_EVENTS_COUNT, ConnectorConstants.OBSRV_SUCCESS_EVENTS_COUNT)
  }

  override def processElement(event: String, context: ProcessFunction[String, String]#Context, metrics: Metrics): Unit = {

    super.initMetrics(connectorCtx.connectorId, connectorCtx.connectorInstanceId)
    metrics.incCounter(connectorCtx.connectorInstanceId, ConnectorConstants.CONNECTOR_SUCCESS_COUNT)

    try {
      validateEvent(event)
      val obsrvEvent = toObsrvEvent(event)
      context.output(ConnectorConstants.SUCCESS_OUTPUT_TAG, obsrvEvent)
      metrics.incCounter(connectorCtx.connectorInstanceId, ConnectorConstants.OBSRV_SUCCESS_EVENTS_COUNT)
    } catch {
      case ex: ObsrvException =>
        metrics.incCounter(connectorCtx.connectorInstanceId, ConnectorConstants.OBSRV_FAILED_EVENTS_COUNT)
        context.output(ConnectorConstants.FAILED_OUTPUT_TAG, markFailed(event, ex.error))
    }
  }

  private def validateEvent(event: String): Unit = {
    val eventSize = event.getBytes("UTF-8").length
    if(eventSize > maxEventSize) throw new ObsrvException(ErrorData("CE_1001", s"Event data exceeded max configured size of $maxEventSize"))
  }

  @throws[ObsrvException]
  private def toObsrvEvent(event: String): String = {
    val eventJson = connectorCtx.dataFormat match {
      case "json" => event
      case _ => throw new ObsrvException(ConnectorConstants.INVALID_DATA_FORMAT_ERROR)
    }
    val syncts = System.currentTimeMillis()
    val obsrvMeta = s"""{"syncts":$syncts,"flags":{},"timespans":{},"error":{},"source":{"connector":${connectorCtx.connectorId},"connectorInstance":${connectorCtx.connectorInstanceId}}}"""
    JSONUtil.getJsonType(eventJson) match {
      case "ARRAY" => s"""{"dataset":"${connectorCtx.datasetId}","events":$eventJson,"obsrv_meta":$obsrvMeta}"""
      case _ => s"""{"dataset":"${connectorCtx.datasetId}","event":$eventJson,"obsrv_meta":$obsrvMeta}"""
    }
  }

  private def markFailed(event: String, error: ErrorData): String = {
    JSONUtil.serialize(Map[String, AnyRef](
      "connector_ctx" -> connectorCtx,
      "event" -> event,
      "error" -> error
    ))
  }
}