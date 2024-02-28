package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sunbird.obsrv.connector.model.Models.ConnectorContext

abstract class ISourceConnector {

  private def addMetric(metricId: String, metricValue: Long) (implicit ctx: ConnectorContext) : Unit = {

  }

  final def process(connectorCtx: ConnectorContext, config: Config, spark: SparkSession) : DataFrame = {
    implicit val ctx : ConnectorContext = connectorCtx
    process(spark, ctx, config, addMetric)
  }

  def getSparkConf(config: Config) : Map[String, String]
  def process(spark: SparkSession, ctx: ConnectorContext, config: Config, metricFn: (String, Long) => Unit) : Dataset[Row]

}
