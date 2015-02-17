package com.socrata.spandex

import com.typesafe.config.Config

class SpandexConfig(conf: Config) {
  val esUrl: String = conf.getString("spandex.elasticsearch.url")
}
