package org.sunbird.obsrv.connector.util

import org.sunbird.obsrv.connector.model.{Metric, MetricContext, MetricData}
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.util.JSONUtil

import java.util.UUID
import scala.collection.mutable

class MetricsCollector(ctx: ConnectorContext) {

  val metricLabels = List(
    Map("key" -> "type", "value" -> "Connector"),
    Map("key" -> "job", "value" -> ctx.connectorId),
    Map("key" -> "instance", "value" -> ctx.connectorInstanceId),
    Map("key" -> "dataset", "value" -> ctx.datasetId)
  )
  val metricContext = MetricContext(pdata = Map("id" -> "Connector", "pid" -> ctx.connectorId))
  val metricActor = Map("id" -> ctx.connectorId, "type" -> "SYSTEM")
  val metricObject = Map("id" -> ctx.datasetId, "type" -> "Dataset")

  val metrics = mutable.Buffer[Metric]()

  def collect(metric: String, value: Long): Unit = {
    collect(Map(metric -> value))
  }

  def collect(metricMap: Map[String, Long]): Unit = {
    metrics.append(generate(metricMap))
  }

  private def generate(metricMap: Map[String, Long]): Metric = {
    Metric(
      eid = "METRIC", ets = System.currentTimeMillis(), mid = UUID.randomUUID().toString,
      actor = metricActor,
      context = metricContext,
      `object` = metricObject,
      edata = MetricData(metric = metricMap, labels = metricLabels)
    )
  }

  def toSeq(): Seq[String] = {
    metrics.map(metric => JSONUtil.serialize(metric)).toSeq
  }

}
