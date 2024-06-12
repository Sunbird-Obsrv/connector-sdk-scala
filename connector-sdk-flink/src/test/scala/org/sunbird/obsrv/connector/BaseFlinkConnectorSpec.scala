package org.sunbird.obsrv.connector

import com.typesafe.config.{Config, ConfigFactory}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.obsrv.connector.source.{IConnectorSource, SourceConnector}
import org.sunbird.obsrv.job.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}

import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import collection.JavaConverters._

abstract class BaseFlinkConnectorSpec extends FlatSpec with BeforeAndAfterAll {

  var embeddedPostgres: EmbeddedPostgres = _

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)

  def testConfiguration(): Configuration = {
    val config = new Configuration()
    config.setString("metrics.reporter", "job_metrics_reporter")
    config.setString("metrics.reporter.job_metrics_reporter.class", classOf[BaseMetricsReporter].getName)
    config
  }

  val config: Config = ConfigFactory.load("connector.conf")
  val postgresConfig: PostgresConnectionConfig = PostgresConnectionConfig(
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"),
    database = "postgres",
    host = config.getString("postgres.host"),
    port = config.getInt("postgres.port"),
    maxConnections = config.getInt("postgres.maxConnections")
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    BaseMetricsReporter.gaugeMetrics.clear()
    embeddedPostgres = EmbeddedPostgres.builder.setPort(config.getInt("postgres.port")).start()
    val postgresConnect = new PostgresConnect(postgresConfig)
    setupConnectorFramework(postgresConnect)
    loadConnectorData(postgresConnect)
    postgresConnect.closeConnection()
    flinkCluster.before()
  }



  override def afterAll(): Unit = {
    super.afterAll()
    embeddedPostgres.close()
    flinkCluster.after()
  }

  private def setupConnectorFramework(postgresConnect: PostgresConnect): Unit = {

    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets ( id text PRIMARY KEY, type text NOT NULL, validation_config json, extraction_config json, dedup_config json, data_schema json, denorm_config json, router_config json NOT NULL, dataset_config json NOT NULL, status text NOT NULL, tags text[], data_version INT, created_by text NOT NULL, updated_by text NOT NULL, created_date timestamp NOT NULL, updated_date timestamp NOT NULL );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS connector_registry ( id TEXT PRIMARY KEY, version TEXT NOT NULL, type TEXT NOT NULL, category TEXT NOT NULL, name TEXT NOT NULL, description TEXT, technology TEXT NOT NULL, licence TEXT NOT NULL, owner TEXT NOT NULL, iconURL TEXT, status TEXT NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date TIMESTAMP NOT NULL DEFAULT now(), updated_date TIMESTAMP NOT NULL, live_date TIMESTAMP NOT NULL DEFAULT now());")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS connector_instances ( id TEXT PRIMARY KEY, dataset_id TEXT NOT NULL REFERENCES datasets (id), connector_id TEXT NOT NULL REFERENCES connector_registry (id), data_format TEXT, connector_config text NOT NULL, operations_config json NOT NULL, status TEXT NOT NULL, connector_state json, connector_stats json, created_by text NOT NULL, updated_by text NOT NULL, created_date TIMESTAMP NOT NULL DEFAULT now(), updated_date TIMESTAMP NOT NULL, published_date TIMESTAMP NOT NULL DEFAULT now());")
  }

  def loadConnectorData(postgresConnect: PostgresConnect): Unit = {

    val cipher = Cipher.getInstance("AES")
    val key = new SecretKeySpec(config.getString("obsrv.encryption.key").getBytes("utf-8"), "AES")
    cipher.init(Cipher.ENCRYPT_MODE, key)
    val encryptedByteValue = cipher.doFinal(JSONUtil.serialize(getSourceConfig()).getBytes("utf-8"))
    val connectorConfig = Base64.getEncoder.encodeToString(encryptedByteValue)

    val connConfig = ConfigFactory.load(getConnectorConfigFile())
    postgresConnect.execute("insert into datasets(id, type, data_schema, validation_config, extraction_config, dedup_config, router_config, dataset_config, status, data_version, created_by, updated_by, created_date, updated_date) values ('d1', 'dataset', '{}', '{\"validate\": false, \"mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}}', '{\"drop_duplicates\": true, \"dedup_key\": \"id\", \"dedup_period\": 3}', '{\"topic\":\"d1-events\"}', '{\"data_key\":\"id\",\"timestamp_key\":\"date\",\"entry_topic\":\"ingest\",\"redis_db_host\":\"localhost\",\"redis_db_port\":3306,\"redis_db\":2}', 'Live', 2, 'System', 'System', now(), now());")
    postgresConnect.execute("insert into connector_registry(id, version, type, category, name, description, technology, licence, owner, status, created_by, updated_by, created_date, updated_date, live_date) " +
      "values ('" + connConfig.getString("metadata.id") + "', '" + connConfig.getString("metadata.version") + "','" + connConfig.getString("metadata.type") + "', '" + connConfig.getString("metadata.category") + "','" + connConfig.getString("metadata.name") + "', '" + connConfig.getString("metadata.description") + "', '" + connConfig.getString("metadata.technology") + "', '" + connConfig.getString("metadata.licence") + "', '" + connConfig.getString("metadata.owner") + "', 'Live', 'System', 'System', now(), now(), now());")
    postgresConnect.execute("insert into connector_instances(id, dataset_id, connector_id, data_format, connector_config, operations_config, status, connector_state, connector_stats, created_by, updated_by, created_date, updated_date, published_date) " +
      "values ('c1', 'd1', '" + connConfig.getString("metadata.id") + "', 'json', '"+ connectorConfig +"','{}', 'Live', '{}', '{}', 'System', 'System', now(), now(), now());")
  }

  def getPrintableMetrics(metricsMap: mutable.Map[String, Long]): Map[String, Map[String, Map[String, Long]]] = {
    metricsMap.map(f => {
      val keys = f._1.split('.')
      val metricValue = f._2
      val jobId = keys.apply(0)
      val datasetId = keys.apply(1)
      val metric = keys.apply(2)
      (jobId, datasetId, metric, metricValue)
    }).groupBy(f => f._1).mapValues(f => f.map(p => (p._2, p._3, p._4))).mapValues(f => f.groupBy(p => p._1).mapValues(q => q.map(r => (r._2, r._3)).toMap))
  }

  def getConnectorConfigFile(): String

  def getConnectorName(): String

  def getConnectorSource(): IConnectorSource

  def getSourceConfig(): Map[String, AnyRef]

  def testFailedEvents(events: java.util.List[String])

  def testSuccessEvents(events: java.util.List[String])

  "BaseFlinkConnectorSpec" should s"test the ${getConnectorName()} connector" in {

    EventsSink.failedEvents.clear()
    EventsSink.successEvents.clear()
    Future {
      SourceConnector.process(Array("--config.file.path", getConnectorConfigFile()), getConnectorSource())(new SuccessSink(), new FailedSink())
    }
    Thread.sleep(10000)

    val failedEvents = EventsSink.failedEvents.asScala.map(f => {
      JSONUtil.deserialize[Map[String,AnyRef]](f).get("event").get.asInstanceOf[String]
    }).asJava
    testFailedEvents(failedEvents)
    val successEvents = EventsSink.successEvents.asScala.map(f => {
      val event = JSONUtil.deserialize[Map[String, AnyRef]](f).get("event").get
      JSONUtil.serialize(event)
    }).asJava
    testSuccessEvents(successEvents)
  }
}
