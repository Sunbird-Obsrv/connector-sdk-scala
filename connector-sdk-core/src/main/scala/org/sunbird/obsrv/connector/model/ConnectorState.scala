package org.sunbird.obsrv.connector.model

import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.job.exception.ObsrvException
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.{JSONUtil, PostgresConnectionConfig}

import scala.collection.mutable


class ConnectorState(postgresConnectionConfig: PostgresConnectionConfig, connectorInstanceId: String, stateJson: Option[String]) {

  private val state: mutable.Map[String, AnyRef] = stateJson match {
    case Some(json) if json != null => JSONUtil.deserialize[mutable.Map[String, AnyRef]](json)
    case _ => mutable.Map[String, AnyRef]()
  }

  def getState[T](attribute: String): Option[T] = {
    state.get(attribute).asInstanceOf[Option[T]]
  }

  def getState[T](attribute: String, defaultValue: T): T = {
    state.get(attribute).map(f => f.asInstanceOf[T]).orElse(Some(defaultValue)).get
  }

  def putState[T <: AnyRef](attrib: String, value: T): Unit = {
    state.put(attrib, value)
  }

  def removeState(attrib: String): Option[AnyRef] = {
    state.remove(attrib)
  }

  def contains(attrib: String) : Boolean = {
    state.contains(attrib)
  }

  def toJson(): String = {
    JSONUtil.serialize(state)
  }

  @throws[ObsrvException]
  def saveState(): Unit = {
    val updCount = ConnectorRegistry.updateConnectorState(postgresConnectionConfig, connectorInstanceId, this.toJson())
    if (updCount != 1) {
      throw new ObsrvException(ErrorData("CONN_STATE_SAVE_FAILED", "Unable to save the connector state"))
    }
  }

}