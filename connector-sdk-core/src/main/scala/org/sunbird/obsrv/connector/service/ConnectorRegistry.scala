package org.sunbird.obsrv.connector.service

import org.sunbird.obsrv.connector.model.Models.{ConnectorContext, ConnectorInstance}
import org.sunbird.obsrv.connector.model.{ConnectorState, ConnectorStats}
import org.sunbird.obsrv.job.util.{JSONUtil, PostgresConnect, PostgresConnectionConfig}

import java.sql.ResultSet

object ConnectorRegistry {
  def getConnectorInstances(connectorId: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Option[List[ConnectorInstance]] = {

    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT ci.*, d.entry_topic, cr.type as connector_type FROM connector_instances as ci JOIN connector_registry cr ON ci.connector_id = cr.id JOIN datasets d ON ci.dataset_id = d.id WHERE ci.connector_id = '$connectorId' AND d.status = 'Live' AND cr.status='Live' AND ci.status = 'Live'")
      Option(Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val datasetSourceConfig = parseConnectorInstance(result)
        datasetSourceConfig
      }).toList)
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def getConnectorInstance(connectorInstanceId: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Option[ConnectorInstance] = {
    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT ci.*, d.dataset_config, cr.type as connector_type FROM connector_instances as ci JOIN connector_registry cr ON ci.connector_id = cr.id JOIN datasets d ON ci.dataset_id = d.id WHERE ci.id = '$connectorInstanceId' AND d.status = 'Live' AND cr.status='Live' AND ci.status = 'Live'")
      if (rs.next()) {
        Some(parseConnectorInstance(rs))
      } else {
        None
      }
    } finally {
      postgresConnect.closeConnection()
    }
  }

  def updateConnectorState(connectorInstanceId: String, state: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Int = {
    val query = s"UPDATE connector_instances SET connector_state = '$state' WHERE id = '$connectorInstanceId'"
    update(query)
  }

  def updateConnectorStats(connectorInstanceId: String, stats: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Int = {
    val query = s"UPDATE connector_instances SET connector_stats = '$stats' WHERE id = '$connectorInstanceId'"
    update(query)
  }

  private def update(query: String)(implicit postgresConnectionConfig: PostgresConnectionConfig): Int = {
    val postgresConnect = new PostgresConnect(postgresConnectionConfig)
    try {
      postgresConnect.executeUpdate(query)
    } finally {
      postgresConnect.closeConnection()
    }
  }

  private def parseConnectorInstance(rs: ResultSet)(implicit postgresConnectionConfig: PostgresConnectionConfig): ConnectorInstance = {
    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val connectorId = rs.getString("connector_id")
    val connectorType = rs.getString("connector_type")
    val dataFormat = rs.getString("data_format")
    val connectorConfig = rs.getString("connector_config")
    val operationsConfig = rs.getString("operations_config")
    val status = rs.getString("status")
    val connectorState = Some(rs.getString("connector_state"))
    val connectorStats = Some(rs.getString("connector_stats"))
    val entryTopic = rs.getString("entry_topic")

    ConnectorInstance(connectorContext = ConnectorContext(connectorId, datasetId, id, connectorType, dataFormat, entryTopic, new ConnectorState(id, connectorState), new ConnectorStats(id, connectorStats)), connectorConfig = connectorConfig, operationsConfig = operationsConfig, status = status)
  }

}
