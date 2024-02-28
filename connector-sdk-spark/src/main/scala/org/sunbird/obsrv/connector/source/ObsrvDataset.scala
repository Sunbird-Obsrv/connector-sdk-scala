package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.spark.sql.Dataset
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.JSONUtil


class ObsrvDataset(ds: Dataset[String]) {

  def filterLargeEvents(ctx: ConnectorContext, config: Config): Dataset[String] = {
    val maxEventSize = config.getInt("kafka.producer.max-request-size")
    ds.filter(event => {
      val eventSize = event.getBytes("UTF-8").length
      eventSize > maxEventSize
    }).map(event => {
      JSONUtil.serialize(Map[String, AnyRef](
        "connector_ctx" -> ctx,
        "event" -> event,
        "error" -> ErrorData("CE_1001", s"Event data exceeded max configured size of $maxEventSize")
      ))
    })(ds.encoder)
  }

  def filterValidEvents(ctx: ConnectorContext, config: Config): Dataset[String] = {
    val maxEventSize = config.getInt("kafka.producer.max-request-size")
    ds.filter(event => {
      val eventSize = event.getBytes("UTF-8").length
      eventSize <= maxEventSize
    }).map(event => {
      val syncts = System.currentTimeMillis()
      val obsrvMeta = s"""{"syncts":$syncts,"flags":{},"timespans":{},"error":{},"source":{"connector":${ctx.connectorId},"connectorInstance":${ctx.connectorInstanceId}}}"""
      s"""{"dataset":"${ctx.datasetId}","event":$event,"obsrv_meta":$obsrvMeta}"""
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