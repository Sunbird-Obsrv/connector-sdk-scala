package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.connector.util.MetricsCollector

abstract class ISourceConnector {

  private def addMetric(metricId: String, metricValue: Long)(implicit collector: MetricsCollector): Unit = {
    collector.collect(metricId, metricValue)
  }

  final def execute(ctx: ConnectorContext, config: Config, spark: SparkSession, metricsCollector: MetricsCollector): DataFrame = {
    implicit val collector: MetricsCollector = metricsCollector
    process(spark, ctx, config, addMetric)
  }

  def getSparkConf(config: Config): Map[String, String]

  def process(spark: SparkSession, ctx: ConnectorContext, config: Config, metricFn: (String, Long) => Unit): Dataset[Row]

}
