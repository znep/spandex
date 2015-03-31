package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

class SpandexConfig(config: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort        = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val es                 = new ElasticSearchConfig(config.getConfig("elastic-search"))

  val suggestFuzziness   = config.getString("suggest-fuzziness")
  val suggestSize        = config.getInt("suggest-size")
}

class ElasticSearchConfig(config: Config) {
  val host               = config.getString("host")
  val port               = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName        = config.getString("cluster-name")
  val index              = config.getString("index")

  val fieldValueMapping  = new MappingConfig(config.getConfig("mappings.field-value"))
  val columnMapMapping   = new MappingConfig(config.getConfig("mappings.column-map"))
  val datasetCopyMapping = new MappingConfig(config.getConfig("mappings.dataset-copy"))

  val dataCopyBatchSize  = config.getInt("data-copy-batch-size")
  val dataCopyTimeout    = config.getDuration("data-copy-timeout", TimeUnit.MILLISECONDS)
}

class MappingConfig(config: Config) {
  val mappingType       = config.getString("mapping-type")
  val mappingProperties = config.getString("mapping-properties")
}
