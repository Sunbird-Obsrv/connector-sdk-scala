package org.sunbird.obsrv.connector.source

import com.typesafe.config.Config
import org.apache.flink.streaming.api.datastream.{SingleOutputStreamOperator, WindowedStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.sunbird.obsrv.job.exception.UnsupportedDataFormatException

import scala.collection.mutable

trait IConnectorSource {

  @throws[UnsupportedDataFormatException]
  def getSourceStream(env: StreamExecutionEnvironment, config: Config): SingleOutputStreamOperator[String]

}

trait IConnectorWindowSource {

  @throws[UnsupportedDataFormatException]
  def getSourceStream[W <: Window](env: StreamExecutionEnvironment, config: Config): WindowedStream[String, String, W]

}