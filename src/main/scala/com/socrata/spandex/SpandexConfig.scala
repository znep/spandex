package com.socrata.spandex

import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

class SpandexConfig(conf: Config = ConfigFactory.load()) {
  val port: Int = conf.getInt("spandex.port")
  val esUrl: String = conf.getString("spandex.elasticsearch.url")
  val index: String = conf.getString("spandex.elasticsearch.index")
  val indexSettings: String = conf.getString("spandex.elasticsearch.indexSettings")
  val indexBaseMapping: String = conf.getString("spandex.elasticsearch.indexBaseMapping")
  val indexColumnMapping: String = conf.getString("spandex.elasticsearch.indexColumnMapping")
  val escTimeoutFast: Duration = Duration(conf.getString("spandex.elasticsearch.esClientTimeoutFast"))
  val escTimeout: Duration = Duration(conf.getString("spandex.elasticsearch.esClientTimeoutContent"))
}
