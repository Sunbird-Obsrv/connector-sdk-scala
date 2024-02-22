package org.sunbird.obsrv.connector.service

import org.sunbird.obsrv.connector.model.Models.{ConnectorContext, ConnectorInstance}
import org.sunbird.obsrv.job.util.{DatasetRegistryConfig, JSONUtil, PostgresConnect}

import java.sql.ResultSet

object ConnectorRegistry {

  def getConnectorInstances(connectorId: String): Option[List[ConnectorInstance]] = {

    val postgresConnect = new PostgresConnect(DatasetRegistryConfig.postgresConfig)
    try {
      val rs = postgresConnect.executeQuery(s"SELECT ci.*, d.dataset_config FROM connector_instances as ci JOIN datasets d ON ci.dataset_id = d.id WHERE ci.connector_id = '$connectorId'")
      Option(Iterator.continually((rs, rs.next)).takeWhile(f => f._2).map(f => f._1).map(result => {
        val datasetSourceConfig = parseConnectorInstance(result)
        datasetSourceConfig
      }).toList)
    } finally {
      postgresConnect.closeConnection()
    }
  }

  private def parseConnectorInstance(rs: ResultSet): ConnectorInstance = {

    val id = rs.getString("id")
    val datasetId = rs.getString("dataset_id")
    val connectorId = rs.getString("connector_id")
    val connectorType = rs.getString("connector_type")
    val dataFormat = rs.getString("data_format")
    val connectorConfig = rs.getString("connector_config")
    val operationsConfig = rs.getString("operations_config")
    val status = rs.getString("status")
    val datasetConfig = rs.getString("dataset_config")
    val configMap = JSONUtil.deserialize[Map[String, AnyRef]](datasetConfig)
    val entryTopic = configMap.get("entry_topic").map(f => f.asInstanceOf[String]).orElse(Some("ingest")).get

    ConnectorInstance(connectorContext = ConnectorContext(connectorId, datasetId, id, connectorType, dataFormat, entryTopic), connectorConfig = connectorConfig, operationsConfig = operationsConfig, status = status)
  }

}
