package org.sunbird.obsrv.connector.model

import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.sunbird.obsrv.job.model.Models.ErrorData


object ConnectorConstants {

  val CONNECTOR_CTX = "connector_ctx"
  val CONNECTOR_SUCCESS_COUNT = "total_connector_success_count"
  val CONNECTOR_FAILED_COUNT = "total_connector_failed_count"
  val SYSTEM_EVENTS_COUNT = "total_system_events_count"
  val OBSRV_FAILED_EVENTS_COUNT = "total_obsrv_failed_count"
  val OBSRV_SUCCESS_EVENTS_COUNT = "total_obsrv_success_count"

  val CONNECTOR_SUCCESS_TAG: OutputTag[String] = OutputTag[String]("connector-success-events")
  val CONNECTOR_FAILED_TAG: OutputTag[String] = OutputTag[String]("connector-failed-events")

  val FAILED_OUTPUT_TAG: OutputTag[String] = OutputTag[String]("failed-events")
  val SUCCESS_OUTPUT_TAG: OutputTag[String] = OutputTag[String]("success-events")
  val SYSTEM_EVENTS_TAG: OutputTag[String] = OutputTag[String]("system-events")

  val INVALID_DATA_FORMAT_ERROR = ErrorData("CE_1000", "Unsupported data format")

}