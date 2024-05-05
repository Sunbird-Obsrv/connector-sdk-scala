package org.sunbird.obsrv.connector.model

import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.job.exception.ObsrvException
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.{JSONUtil, PostgresConnectionConfig}

import scala.collection.mutable

class ConnectorStats(postgresConnectionConfig: PostgresConnectionConfig, connectorInstanceId: String, statsJson: Option[String]) {

  private val stats: mutable.Map[String, AnyRef] = statsJson match {
    case Some(json) if json != null => JSONUtil.deserialize[mutable.Map[String, AnyRef]](json)
    case _ => mutable.Map[String, AnyRef]()
  }

  def getStat[T](metric: String): Option[T] = {
    stats.get(metric).asInstanceOf[Option[T]]
  }

  def getStat[T](metric: String, defaultValue: T): T = {
    stats.get(metric).map(f => f.asInstanceOf[T]).orElse(Some(defaultValue)).get
  }

  def putStat[T <: AnyRef](metric: String, value: T): Unit = {
    stats.put(metric, value)
  }

  def removeStat(metric: String): Option[AnyRef] = {
    stats.remove(metric)
  }

  def toJson(): String = {
    JSONUtil.serialize(stats)
  }

  @throws[ObsrvException]
  def saveStats(): Unit = {
    val updCount = ConnectorRegistry.updateConnectorStats(postgresConnectionConfig, connectorInstanceId, this.toJson())
    if (updCount != 1) {
      throw new ObsrvException(ErrorData("CONN_STATS_SAVE_FAILED", "Unable to save the connector stats"))
    }
  }

}