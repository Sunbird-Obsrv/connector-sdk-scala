package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.flink.streaming.api.datastream.{SingleOutputStreamOperator, WindowedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.Window
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.job.exception.UnsupportedDataFormatException

trait IConnectorSource extends Serializable {

  @throws[UnsupportedDataFormatException]
  def getSourceStream(env: StreamExecutionEnvironment, config: Config): SingleOutputStreamOperator[String]

  def getSourceFunction(contexts: List[ConnectorContext], config: Config): SourceConnectorFunction
}

trait IConnectorWindowSource[W <: Window] {

  @throws[UnsupportedDataFormatException]
  def getSourceStream(env: StreamExecutionEnvironment, config: Config): WindowedStream[String, String, W]

  def getSourceFunction(contexts: List[ConnectorContext], config: Config): SourceConnectorWindowFunction[W]

}