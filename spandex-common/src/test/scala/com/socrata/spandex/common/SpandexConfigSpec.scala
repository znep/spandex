package com.socrata.spandex.common

import org.scalatest.{Matchers, FunSuiteLike}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("config has these required values") {
    val conf = new SpandexConfig
    (
      conf.spandexPort,
      conf.esPort,
      conf.clusterName,
      conf.clusterTimeout,
      conf.esUrl(0),
      conf.index,
      conf.indexSettings,
      conf.indexBaseMapping,
      conf.indexColumnMapping,
      conf.escTimeout,
      conf.escTimeoutFast,
      conf.bulkBatchSize
    )
  }
}
