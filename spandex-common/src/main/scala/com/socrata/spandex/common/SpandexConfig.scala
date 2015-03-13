package com.socrata.spandex.common

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.Duration

class SpandexConfig(conf: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort: Int = conf.getInt("port")
  val esPort: Int = conf.getInt("elasticsearch.port")
  val clusterName: String = conf.getString("elasticsearch.clusterName")
  val clusterTimeout: Long = conf.getLong("elasticsearch.clusterTimeout")
  def esUrl(port: Int): String = s"http://localhost:$port"
  val index: String = conf.getString("elasticsearch.index")
  val indexSettings: String = conf.getConfig("elasticsearch.indexSettings").toString
  val indexBaseMapping: String = conf.getString("elasticsearch.indexBaseMapping")
  val indexColumnMapping: String = conf.getString("elasticsearch.indexColumnMapping")
  // TODO: use socrata-thirdparty-utils ConfigClass getDuration
  val escTimeoutFast: Duration = Duration(conf.getString("elasticsearch.esClientTimeoutFast"))
  val escTimeout: Duration = Duration(conf.getString("elasticsearch.esClientTimeoutContent"))
  val bulkBatchSize: Int = conf.getInt("elasticsearch.bulkBatchSize")
}
