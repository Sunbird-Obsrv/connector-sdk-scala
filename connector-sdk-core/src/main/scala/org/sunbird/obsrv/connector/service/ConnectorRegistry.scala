package org.sunbird.obsrv.connector.service

import org.sunbird.obsrv.connector.model.Models.{ConnectorContext, ConnectorInstance}
import org.sunbird.obsrv.connector.model.{ConnectorState, ConnectorStats}
import org.sunbird.obsrv.job.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}

import java.sql.ResultSet

object ConnectorRegistry {

  def getConnectorInstances(postgresConnectionConfig: PostgresConnectionConfig, connectorId: String): Option[List[ConnectorInstance]] = {

    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT ci.*, d.dataset_config FROM connector_instances as ci JOIN datasets d ON ci.dataset_id = d.id WHERE ci.connector_id = '$connectorId' AND d.status = 'Live' AND ci.status = 'Live'")
      Option(Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val datasetSourceConfig = parseConnectorInstance(postgresConnectionConfig, result)
        datasetSourceConfig
      }).toList)
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def getConnectorInstance(postgresConnectionConfig: PostgresConnectionConfig, connectorInstanceId: String): Option[ConnectorInstance] = {
    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT ci.*, d.dataset_config FROM connector_instances as ci JOIN datasets d ON ci.dataset_id = d.id WHERE ci.id = '$connectorInstanceId' AND d.status = 'Live' AND ci.status = 'Live'")
      if (rs.next()) {
        Some(parseConnectorInstance(postgresConnectionConfig, rs))
      } else {
        None
      }
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def updateConnectorState(postgresConnectionConfig: PostgresConnectionConfig, connectorInstanceId: String, state: String): Int = {
    val query = s"UPDATE connector_instances SET connector_state = '$state' WHERE id = '$connectorInstanceId'"
    update(postgresConnectionConfig, query)
  }

  def updateConnectorStats(postgresConnectionConfig: PostgresConnectionConfig, connectorInstanceId: String, stats: String): Int = {
    val query = s"UPDATE connector_instances SET connector_stats = '$stats' WHERE id = '$connectorInstanceId'"
    update(postgresConnectionConfig, query)
  }

  private def update(postgresConnectionConfig: PostgresConnectionConfig, query: String): Int = {
    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      postgresConnect.executeUpdate(query)
    } finally {
      postgresConnect.closeConnection()
    }
  }

  private def parseConnectorInstance(postgresConnectionConfig: PostgresConnectionConfig, rs: ResultSet): ConnectorInstance = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val connectorId = rs.getString("connector_id")
    val connectorType = rs.getString("connector_type")
    val dataFormat = rs.getString("data_format")
    val connectorConfig = rs.getString("connector_config")
    val operationsConfig = rs.getString("operations_config")
    val status = rs.getString("status")
    val datasetConfig = rs.getString("dataset_config")
    val connectorState = Some(rs.getString("connector_state"))
    val connectorStats = Some(rs.getString("connector_stats"))
    val configMap = JSONUtil.deserialize[Map[String, AnyRef]](datasetConfig)
    val entryTopic = configMap.get("entry_topic").map(f => f.asInstanceOf[String]).orElse(Some("ingest")).get

    ConnectorInstance(connectorContext = ConnectorContext(connectorId, datasetId, id, connectorType, dataFormat, entryTopic, new ConnectorState(postgresConnectionConfig, id, connectorState), new ConnectorStats(postgresConnectionConfig, id, connectorStats)), connectorConfig = connectorConfig, operationsConfig = operationsConfig, status = status)
    }

}
