package com.socrata.spandex.common

import com.typesafe.config.{Config, ConfigFactory}

class SpandexConfig(conf: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort: Int = conf.getInt("port") // scalastyle:ignore multiple.string.literals
  val es = new ElasticSearchConfig(conf.getConfig("elastic-search"))
}

class ElasticSearchConfig(config: Config) {
  val host               = config.getString("host")
  val port               = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName        = config.getString("cluster-name")
  val index              = config.getString("index")

  val fieldValueMapping  = new MappingConfig(config.getConfig("mappings.field-value"))
  val columnMapMapping   = new MappingConfig(config.getConfig("mappings.column-map"))
  val datasetCopyMapping = new MappingConfig(config.getConfig("mappings.dataset-copy"))
}

class MappingConfig(config: Config) {
  val mappingType       = config.getString("mapping-type")
  val mappingProperties = config.getString("mapping-properties")
}
