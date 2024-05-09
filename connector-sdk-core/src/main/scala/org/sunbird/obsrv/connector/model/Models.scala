package org.sunbird.obsrv.connector.model

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import org.sunbird.obsrv.job.model.Models.ErrorData

object Models {

  case class ConnectorContext(
     @JsonProperty("connector_id") connectorId: String,
     @JsonProperty("dataset_id") datasetId: String,
     @JsonProperty("connector_instance_id") connectorInstanceId: String,
     @JsonProperty("connector_type") connectorType: String,
     @JsonProperty("data_format") dataFormat: String,
     @JsonIgnore entryTopic: String,
     @JsonIgnore state: ConnectorState,
     @JsonIgnore stats: ConnectorStats
   )

  case class ConnectorInstance(connectorContext: ConnectorContext, connectorConfig: String, operationsConfig: String, status: String) extends Serializable

  case class ErrorEvent(event: String, error: ErrorData)

}