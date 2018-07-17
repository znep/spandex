package com.socrata.spandex.secondary

import com.typesafe.config.{ConfigFactory, Config}

import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.{Eventually, RefreshPolicy, SpandexElasticSearchClient}

class SpandexSecondary(
    override val client: SpandexElasticSearchClient,
    override val index: String,
    val batchSize: Int,
    override val maxValueLength: Int,
    override val resyncBatchSize: Int,
    override val refresh: RefreshPolicy = Eventually)
  extends SpandexSecondaryLike {

  def this(config: SpandexConfig) = {
    this(
      new SpandexElasticSearchClient(config.es),
      config.es.index,
      config.es.dataCopyBatchSize,
      config.es.maxColumnValueLength,
      config.resyncBatchSize
    )
  }

  // Use any config we are given by the secondary watcher, falling back to our locally defined config if not specified
  // The SecondaryWatcher isn't setting the context class loader, so for now we tell ConfigFactory what classloader
  // to use so we can actually find the config in our jar.
  def this(rawConfig: Config) = {
    this(new SpandexConfig(
           rawConfig.withFallback(
             ConfigFactory.load(classOf[SpandexSecondary].getClassLoader).getConfig("com.socrata.spandex"))))
  }

  def shutdown(): Unit = client.close()
}
