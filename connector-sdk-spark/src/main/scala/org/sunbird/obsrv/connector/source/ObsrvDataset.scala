package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.spark.sql.Dataset
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.JSONUtil


class ObsrvDataset(ds: Dataset[String]) {
  def getObsrvMeta(ctx: ConnectorContext, errorData: Map[String, String] = Map[String, String]()): String = {
    val syncts = System.currentTimeMillis()
    JSONUtil.serialize(Map[String, AnyRef](
      "syncts" -> s"$syncts",
      "flags" -> Map[String, AnyRef](),
      "timespans" -> Map[String, AnyRef](),
      "error" -> errorData,
      "source" -> Map[String, AnyRef](
        "connector" -> ctx.connectorId,
        "connectorInstance" -> ctx.connectorInstanceId
      )
    ))
  }

  def filterLargeEvents(ctx: ConnectorContext, config: Config): Dataset[String] = {
    val maxEventSize = config.getInt("kafka.producer.max-request-size")
    val obsrvMeta = getObsrvMeta(ctx, Map[String, String]("errCode" -> "CE_1001", "errMsg" -> s"Event data exceeded max configured size of $maxEventSize"))
    val dataset = ctx.datasetId
    ds.filter(event => {
      val eventSize = event.getBytes("UTF-8").length
      eventSize > maxEventSize
    }).map(event => {
      s"""{"event":$event, "dataset":"$dataset", "obsrv_meta":$obsrvMeta}"""
    })(ds.encoder)
  }

  def filterValidEvents(ctx: ConnectorContext, config: Config): Dataset[String] = {
    val maxEventSize = config.getInt("kafka.producer.max-request-size")
    val obsrvMeta = getObsrvMeta(ctx)
    val dataset = ctx.datasetId
    ds.filter(event => {
      val eventSize = event.getBytes("UTF-8").length
      eventSize <= maxEventSize
    }).map(event => {
      s"""{"event":$event, "dataset":"$dataset", "obsrv_meta":$obsrvMeta}"""
    })(ds.encoder)
  }

  def saveToKafka(config: Config, topic: String) = {
    ds.write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("kafka.producer.broker-servers"))
      .option("kafka.compression.type", if (config.hasPath("kafka.producer.compression")) config.getString("kafka.producer.compression") else "snappy")
      .option("topic", topic)
      .save()
  }

}

object DatasetUtil {
  implicit def extensions(df: Dataset[String]) = new ObsrvDataset(df);
}