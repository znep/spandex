package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

class SpandexConfig(config: Config = ConfigFactory.load().getConfig("com.socrata.spandex")) {
  val spandexPort = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val es = new ElasticSearchConfig(config.getConfig("elastic-search"))
  val suggestSize = config.getInt("suggest-size")
  val resyncBatchSize = config.getInt("resync-batch-size")

  val debugString = config.root.render()
}

class ElasticSearchConfig(config: Config) {
  def optionally[T](e: => T): Option[T] = try {
    Some(e)
  } catch {
    case _: ConfigException.Missing => None
  }

  val host = config.getString("host")
  val port = config.getInt("port") // scalastyle:ignore multiple.string.literals
  val clusterName = config.getString("cluster-name")
  val index = config.getString("index")
  val username = optionally(config.getString("username"))
  val password = optionally(config.getString("password"))
  val dataCopyBatchSize = config.getInt("data-copy-batch-size")
  val dataCopyTimeout = config.getDuration("data-copy-timeout", TimeUnit.MILLISECONDS)
  val maxColumnValueLength = config.getInt("max-input-length")
}
