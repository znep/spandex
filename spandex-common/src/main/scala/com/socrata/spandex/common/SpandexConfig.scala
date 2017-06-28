package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}

class SpandexConfig(config: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort        = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val es                 = new ElasticSearchConfig(config.getConfig("elastic-search"))
  val analysis           = new AnalysisConfig(config.getConfig("analysis"))

  val suggestFuzziness   = config.getString("suggest-fuzziness")
  val suggestFuzzLength  = config.getInt("suggest-fuzziness-length")
  val suggestFuzzPrefix  = config.getInt("suggest-fuzziness-prefix")
  val suggestSize        = config.getInt("suggest-size")

  val debugString = config.root.render()
}

class AnalysisConfig(config: Config) {
  val enabled            = config.getBoolean("enabled")
  val luceneVersion      = config.getString("lucene-version")
  val maxInputLength     = config.getInt("max-input-length")
  val maxShingleLength   = config.getInt("max-shingle-length")
}

class ElasticSearchConfig(config: Config) {
  val host               = config.getString("host")
  val port               = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName        = config.getString("cluster-name")
  val index              = config.getString("index")

  val dataCopyBatchSize  = config.getInt("data-copy-batch-size")
  val dataCopyTimeout    = config.getDuration("data-copy-timeout", TimeUnit.MILLISECONDS)
}

class MappingConfig(config: Config) {
  val mappingType        = config.getString("mapping-type")
  val mappingProperties  = config.getString("mapping-properties")
}
