package org.sunbird.obsrv.connector.model

case class Metric(eid: String, ets: Long, mid: String, actor: Map[String, String], context: MetricContext,
                  `object`: Map[String, String], edata: MetricData)
case class MetricContext(pdata: Map[String, String])
case class MetricData(metric: Map[String, Long], labels: List[Map[String, String]])

case class ExecutionMetric(totalRecords: Long, failedRecords: Long, successRecords: Long, connectorExecTime: Long, frameworkExecTime: Long, totalExecTime: Long) {
  def toMetric(): Map[String, Long] = {
    Map(
      "total_records_count" -> totalRecords,
      "failed_records_count" -> failedRecords,
      "success_records_count" -> successRecords,
      "total_exec_time_ms" -> totalExecTime,
      "connector_exec_time_ms" -> connectorExecTime,
      "fw_exec_time_ms" -> frameworkExecTime
    )
  }
}
