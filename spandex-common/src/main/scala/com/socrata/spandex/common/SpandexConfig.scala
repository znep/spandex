package com.socrata.spandex.common

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.Duration

class SpandexConfig(conf: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort: Int = conf.getInt("port") // scalastyle:ignore multiple.string.literals
  val es = new ElasticSearchConfig(conf.getConfig("elasticsearch"))
  val clusterTimeout: Long = conf.getLong("elasticsearch.clusterTimeout")
  def esUrl(port: Int): String = s"http://localhost:$port"
  val indexSettings: String = conf.getConfig("elasticsearch.indexSettings").toString
  val indexColumnMapping: String = conf.getString("elasticsearch.indexColumnMapping")
  // TODO: use socrata-thirdparty-utils ConfigClass getDuration
  val escTimeoutFast: Duration = Duration(conf.getString("elasticsearch.esClientTimeoutFast"))
  val escTimeout: Duration = Duration(conf.getString("elasticsearch.esClientTimeoutContent"))
  val bulkBatchSize: Int = conf.getInt("elasticsearch.bulkBatchSize")
}

class ElasticSearchConfig(config: Config) {
  val host               = config.getString("host")
  val port               = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName        = config.getString("clusterName")
  val index              = config.getString("index")
  val fieldValueMapping  = new MappingConfig(config.getConfig("fieldValueMapping"))
  val datasetCopyMapping = new MappingConfig(config.getConfig("datasetCopyMapping"))
}

class MappingConfig(config: Config) {
  val mappingType       = config.getString("mappingType")
  val mappingProperties = config.getString("mappingProperties")
}
