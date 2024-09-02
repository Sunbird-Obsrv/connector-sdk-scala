package org.sunbird.obsrv.connector.source

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.{SingleOutputStreamOperator, WindowedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.slf4j.LoggerFactory
import org.sunbird.obsrv.connector.model.ConnectorConstants
import org.sunbird.obsrv.connector.model.Models._
import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.connector.util.EncryptionUtil
import org.sunbird.obsrv.job.exception.ObsrvException
import org.sunbird.obsrv.job.util._

import java.io.File

import scala.collection.mutable

object SourceConnector {

  private[this] val logger = LoggerFactory.getLogger(SourceConnector.getClass)

  private def getConfig(args: Array[String]): Config = {
    val configFilePathOpt = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val configFilePath = configFilePathOpt.getOrElse("config.json")
    val configFile = new File(configFilePath)
    val config: Config = if (configFile.exists()) {
      println("Loading configuration file from path: " + configFilePath + "...")
      ConfigFactory.parseFile(configFile).resolve()
    } else {
      println("Loading configuration file connector.conf inside the jar...")
      ConfigFactory.load("connector.conf").withFallback(ConfigFactory.systemEnvironment())
    }
    config
    // ConfigFactory.load(configFilePath).withFallback(ConfigFactory.load("connector.conf")).withFallback(ConfigFactory.systemEnvironment())
  }

  def process(args: Array[String], connectorSource: IConnectorSource)
             (implicit successSink: SinkFunction[String] = null, failedSink: SinkFunction[String] = null): Unit = {
    val config = getConfig(args)
    println("config: " + config)
    implicit val pgConfig: PostgresConnectionConfig = DatasetRegistryConfig.getPostgresConfig(ParameterTool.fromArgs(args).get("config.file.path"))
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val kc: FlinkKafkaConnector = if (successSink == null) new FlinkKafkaConnector(config) else null
    implicit val encUtil: EncryptionUtil = new EncryptionUtil(config.getString("obsrv.encryption.key"))
    val connectorInstancesMap = getConnectorInstances(config)
    connectorInstancesMap.foreach(entry => {
      val connectorConfig = getConnectorConfig(entry._1, config)
      try {
        processConnectorInstance(connectorSource, entry._2.toList, connectorConfig)(env, kc, successSink, failedSink)
      } catch {
        case ex: ObsrvException =>
          logger.error(s"Unable to process connector instance | connectorCtx: ${JSONUtil.serialize(entry._2.toList)} | error: ${JSONUtil.serialize(ex.error)}", ex)
        // TODO: How to raise an event for alerts?
      }
    })
    env.execute(config.getString("metadata.id"))
  }

  def processWindow[W <: Window](args: Array[String], connectorSource: IConnectorWindowSource[W])
                                (implicit successSink: SinkFunction[String] = null, failedSink: SinkFunction[String] = null): Unit = {
    val config = getConfig(args)
    implicit val pgConfig: PostgresConnectionConfig = DatasetRegistryConfig.getPostgresConfig(ParameterTool.fromArgs(args).get("config.file.path"))
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val kc: FlinkKafkaConnector = if (successSink == null) new FlinkKafkaConnector(config) else null
    implicit val encUtil: EncryptionUtil = new EncryptionUtil(config.getString("obsrv.encryption.key"))
    val connectorInstancesMap = getConnectorInstances(config)
    connectorInstancesMap.foreach(entry => {
      val connectorConfig = getConnectorConfig(entry._1, config)
      try {
        processConnectorInstanceWindow(connectorSource, entry._2.toList, connectorConfig)(env, kc, successSink, failedSink)
      } catch {
        case ex: ObsrvException =>
          logger.error(s"Unable to process connector instance | connectorCtx: ${JSONUtil.serialize(entry._2.toList)} | error: ${JSONUtil.serialize(ex.error)}", ex)
        // TODO: How to raise an event for alerts?
      }
    })
    env.execute(config.getString("metadata.id"))
  }

  private def processConnectorInstanceWindow[W <: Window](connectorSource: IConnectorWindowSource[W], connectorContexts: List[ConnectorContext], config: Config)
                                                         (implicit env: StreamExecutionEnvironment, kafkaConnector: FlinkKafkaConnector, successSink: SinkFunction[String], failedSink: SinkFunction[String]): Unit = {

    logger.info("[Start] Register connector instance streams")
    val sourceStream: WindowedStream[String, String, W] = connectorSource.getSourceStream(env, config)
    val dataStream = sourceStream.process(connectorSource.getSourceFunction(connectorContexts, config))

    connectorContexts.foreach(connectorCtx => {
      processSuccessStream(dataStream, connectorCtx, config)(kafkaConnector, successSink, failedSink)
      processFailedStream(dataStream, connectorCtx, config)(kafkaConnector, failedSink)
    })
    logger.info("[End] Register connector instance streams")
  }

  private def processConnectorInstance(connectorSource: IConnectorSource, connectorContexts: List[ConnectorContext], config: Config)
                                      (implicit env: StreamExecutionEnvironment, kafkaConnector: FlinkKafkaConnector, successSink: SinkFunction[String], failedSink: SinkFunction[String]): Unit = {

    logger.info("[Start] Register connector instance streams")
    val sourceStream = connectorSource.getSourceStream(env, config).setParallelism(config.getInt("task.consumer.parallelism")).rebalance()
    val dataStream = sourceStream.process(connectorSource.getSourceFunction(connectorContexts, config))

    connectorContexts.foreach(connectorCtx => {
      processSuccessStream(dataStream, connectorCtx, config)(kafkaConnector, successSink, failedSink)
      processFailedStream(dataStream, connectorCtx, config)(kafkaConnector, failedSink)
    })
    logger.info("[End] Register connector instance streams")
  }

  private def processFailedStream(dataStream: SingleOutputStreamOperator[String], connectorCtx: ConnectorContext, config: Config)
                                 (implicit kafkaConnector: FlinkKafkaConnector, failedSink: SinkFunction[String]): Unit = {

    val failedSinkId = s"${connectorCtx.datasetId}-${connectorCtx.connectorId}-failed"
    val failedStream = dataStream.getSideOutput(ConnectorConstants.CONNECTOR_FAILED_TAG).process(new FailedEventFunction(connectorCtx))

    if (failedSink == null) {
      failedStream.getSideOutput(ConnectorConstants.FAILED_OUTPUT_TAG)
        .sinkTo(kafkaConnector.kafkaSink[String](config.getString("kafka.output.connector.failed.topic"))).name(failedSinkId).uid(failedSinkId)
        .setParallelism(config.getInt("task.downstream.operators.parallelism"))
    } else {
      failedStream.getSideOutput(ConnectorConstants.FAILED_OUTPUT_TAG).addSink(failedSink)
    }
  }

  private def processSuccessStream(dataStream: SingleOutputStreamOperator[String], connectorCtx: ConnectorContext, config: Config)
                                  (implicit kafkaConnector: FlinkKafkaConnector, successSink: SinkFunction[String], failedSink: SinkFunction[String]): Unit = {

    val successSinkId = s"${connectorCtx.datasetId}-${connectorCtx.connectorId}-success"
    val failedSinkId = s"${connectorCtx.datasetId}-${connectorCtx.connectorId}-obsrv-failed"
    val downstreamOperatorParallelism = config.getInt("task.downstream.operators.parallelism")

    val successStream = dataStream.getSideOutput(ConnectorConstants.CONNECTOR_SUCCESS_TAG).process(new SuccessEventFunction(connectorCtx, config))

    if (successSink == null) {
      successStream.getSideOutput(ConnectorConstants.SUCCESS_OUTPUT_TAG)
        .sinkTo(kafkaConnector.kafkaSink[String](connectorCtx.entryTopic)).name(successSinkId).uid(successSinkId)
        .setParallelism(downstreamOperatorParallelism)
      successStream.getSideOutput(ConnectorConstants.FAILED_OUTPUT_TAG)
        .sinkTo(kafkaConnector.kafkaSink[String](config.getString("kafka.output.connector.failed.topic"))).name(failedSinkId).uid(failedSinkId)
        .setParallelism(downstreamOperatorParallelism)
    } else {
      successStream.getSideOutput(ConnectorConstants.SUCCESS_OUTPUT_TAG).addSink(successSink)
      successStream.getSideOutput(ConnectorConstants.FAILED_OUTPUT_TAG).addSink(failedSink)
    }

  }

  private def getConnectorConfig(connectorInstance: ConnectorInstance, config: Config)(implicit encryptionUtil: EncryptionUtil): Config = {
    ConfigFactory.parseString(encryptionUtil.decrypt(connectorInstance.connectorConfig))
      .withFallback(ConfigFactory.parseString(connectorInstance.operationsConfig))
      .withFallback(config)
  }

  private def getConnectorInstances(config: Config)(implicit encryptionUtil: EncryptionUtil, postgresConnectionConfig: PostgresConnectionConfig): mutable.Map[ConnectorInstance, mutable.ListBuffer[ConnectorContext]] = {
    val connectorInstances = ConnectorRegistry.getConnectorInstances(config.getString("metadata.id"))
    connectorInstances.map(instances => {
      val connConfigList = mutable.ListBuffer[Map[String, AnyRef]]()
      val connectorInstanceMap = mutable.Map[ConnectorInstance, mutable.ListBuffer[ConnectorContext]]()
      instances.foreach(instance => {
        val connConfig = JSONUtil.deserialize[Map[String, AnyRef]](encryptionUtil.decrypt(instance.connectorConfig))
        if (connConfigList.contains(connConfig)) { // Same source pointing to two datasets
          connectorInstanceMap(instance).append(instance.connectorContext)
        } else {
          connConfigList.append(connConfig)
          connectorInstanceMap.put(instance, mutable.ListBuffer(instance.connectorContext))
        }
      })
      connectorInstanceMap
    }).orElse(Some(mutable.Map[ConnectorInstance, mutable.ListBuffer[ConnectorContext]]())).get
  }
}