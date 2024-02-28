package org.sunbird.obsrv.connector.model

import org.sunbird.obsrv.connector.service.ConnectorRegistry
import org.sunbird.obsrv.job.exception.ObsrvException
import org.sunbird.obsrv.job.model.Models.ErrorData
import org.sunbird.obsrv.job.util.JSONUtil

import scala.collection.mutable


class ConnectorState(connectorInstanceId: String, stateJson: Option[String]) {

  private val state: mutable.Map[String, AnyRef] = stateJson.map(json => {
    JSONUtil.deserialize[mutable.Map[String, AnyRef]](json)
  }).orElse(Some(mutable.Map[String, AnyRef]())).get

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
    val updCount = ConnectorRegistry.updateConnectorState(connectorInstanceId, this.toJson())
    if (updCount != 1) {
      throw new ObsrvException(ErrorData("CONN_STATE_SAVE_FAILED", "Unable to save the connector state"))
    }
  }

}