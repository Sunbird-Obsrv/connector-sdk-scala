package org.sunbird.obsrv.connector.source

import com.typesafe.config.{Config, ConfigFactory}
import mainargs.{ParserForClass, arg, main}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.sunbird.obsrv.connector.model.ExecutionMetric
import org.sunbird.obsrv.connector.model.Models.{ConnectorContext, ConnectorInstance}
import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.connector.source.DatasetUtil.extensions
import org.sunbird.obsrv.connector.util.{EncryptionUtil, MetricsCollector}
import org.sunbird.obsrv.job.util.{DatasetRegistryConfig, PostgresConnectionConfig, Util}

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
    implicit val postgresConnectionConfig: PostgresConnectionConfig = DatasetRegistryConfig.getPostgresConfig(connectorArgs.configFilePath)
    implicit val encryptionUtil: EncryptionUtil = new EncryptionUtil(config.getString("obsrv.encryption.key"))
    val connectorInstanceOpt = getConnectorInstance(connectorArgs.connectorInstanceId)
    if (connectorInstanceOpt.isDefined) {
      val connectorInstance = connectorInstanceOpt.get
      val connectorConfig = getConnectorConfig(connectorInstance, config)
      val ctx = connectorInstance.connectorContext
      val metricsCollector = new MetricsCollector(ctx)
      implicit val spark: SparkSession = getSparkSession(ctx, connectorConfig, connector.getSparkConf(config))
      import spark.implicits.newStringEncoder
      val executionMetric = processConnector(connector, ctx, connectorConfig, metricsCollector)
      metricsCollector.collect(metricMap = executionMetric.toMetric())
      spark.createDataset(metricsCollector.toSeq()).saveToKafka(connectorConfig, config.getString("kafka.output.connector.metric.topic"))
      spark.close()
    }
  }

  private def processConnector(connector: ISourceConnector, ctx:ConnectorContext, config: Config, metricsCollector: MetricsCollector)(implicit spark: SparkSession): ExecutionMetric = {
    val res = Util.time({
      val res1 = Util.time({
        val df = connector.execute(ctx, config, spark, metricsCollector).toJSON
        val totalRecords = df.count()
        (df, totalRecords)
      })
      val dataset = res1._2._1
      val res2 = Util.time({
        val failedEvents = dataset.filterLargeEvents(ctx, config)
        val validEvents = dataset.filterValidEvents(ctx, config)
        val failedRecordsCount = failedEvents.count()
        val validRecordsCount = validEvents.count()
        failedEvents.saveToKafka(config, config.getString("kafka.output.connector.failed.topic"))
        validEvents.saveToKafka(config, ctx.entryTopic)
        ctx.state.saveState()
        ctx.stats.saveStats()
        (failedRecordsCount, validRecordsCount)
      })
      (res1._2._2, res2._2._1, res2._2._2, res1._1, res2._1)
    })
    ExecutionMetric(res._2._1, res._2._2, res._2._3, res._2._4, res._2._5, res._1)
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

  private def getConnectorInstance(connectorInstanceId: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Option[ConnectorInstance] = {
    ConnectorRegistry.getConnectorInstance(connectorInstanceId)
  }

  private def getConnectorConfig(connectorInstance: ConnectorInstance, config: Config)(implicit encryptionUtil: EncryptionUtil): Config = {
    ConfigFactory.parseString(encryptionUtil.decrypt(connectorInstance.connectorConfig))
      .withFallback(ConfigFactory.parseString(connectorInstance.operationsConfig))
      .withFallback(config)
  }

}
