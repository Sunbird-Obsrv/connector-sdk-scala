package org.sunbird.obsrv.connector.source

import com.typesafe.config.{Config, ConfigFactory}
import mainargs.{ParserForClass, arg, main}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.sunbird.obsrv.connector.model.Models.{ConnectorContext, ConnectorInstance}
import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.connector.source.DatasetUtil.extensions
import org.sunbird.obsrv.connector.util.EncryptionUtil

import java.io.File

object SourceConnector {

  @main
  private case class ConnectorArgs(@arg(short = 'f', name = "config.file.path", doc = "Config file path")
                                   configFilePath: String,
                                   @arg(short = 'c', name = "connector.instance.id", doc = "Connector instance id")
                                   connectorInstanceId: String)

  private def getConfig(connectorArgs: ConnectorArgs): Config = {
    val configFilePath = Option(connectorArgs.configFilePath)
    configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("config.json")
      .withFallback(ConfigFactory.load("connector.conf"))
      .withFallback(ConfigFactory.systemEnvironment()))
  }

  def process(args: Array[String], connector: ISourceConnector): Unit = {

    val connectorArgs = ParserForClass[ConnectorArgs].constructOrExit(args)
    val config = getConfig(connectorArgs)
    implicit val encryptionUtil: EncryptionUtil = new EncryptionUtil(config.getString("obsrv.encryption.key"))
    val connectorInstanceOpt = getConnectorInstance(connectorArgs.connectorInstanceId)
    if (connectorInstanceOpt.isDefined) {
      val connectorInstance = connectorInstanceOpt.get
      val connectorConfig = getConnectorConfig(connectorInstance, config)
      val ctx = connectorInstance.connectorContext
      val spark = getSparkSession(ctx, config, connector.getSparkConf(config))
      val dataset = connector.process(ctx, connectorConfig, spark).toJSON
      dataset.filterLargeEvents(ctx, config).saveToKafka(config, config.getString("kafka.output.connector.failed.topic"))
      dataset.filterValidEvents(ctx, config).saveToKafka(config, ctx.entryTopic)
      spark.close()
    }
  }

  private def getSparkSession(connectorContext: ConnectorContext, config: Config, sparkConfig: Map[String, String]): SparkSession = {

    // TODO: Add all possible global spark configurations
    val conf = new SparkConf()
      .setAppName(s"${connectorContext.connectorId}_${connectorContext.connectorInstanceId}")
      .setMaster(config.getString("spark.master"))
      .set("spark.executor.allowSparkContext", "true")
      .set("spark.driver.allowSparkContext", "true")
      .setAll(sparkConfig)
    SparkSession.builder().config(conf).getOrCreate()
  }

  private def getConnectorInstance(connectorInstanceId: String): Option[ConnectorInstance] = {
    ConnectorRegistry.getConnectorInstance(connectorInstanceId)
  }

  private def getConnectorConfig(connectorInstance: ConnectorInstance, config: Config)(implicit encryptionUtil: EncryptionUtil): Config = {
    ConfigFactory.parseString(encryptionUtil.decrypt(connectorInstance.connectorConfig))
      .withFallback(ConfigFactory.parseString(connectorInstance.operationsConfig))
      .withFallback(config)
  }

}
