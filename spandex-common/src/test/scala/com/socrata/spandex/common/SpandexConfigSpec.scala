package com.socrata.spandex.common

import org.scalatest.{FunSuiteLike, Matchers}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("spandex config has these required values") {
    val conf = new SpandexConfig
    conf.spandexPort
  }

  test("elasticsearch config has these required values") {
    val conf = new SpandexConfig().es
    (
      conf.host,
      conf.port,
      conf.clusterName,
      conf.index,
      conf.fieldValueMapping,
      conf.columnMapMapping,
      conf.datasetCopyMapping
      )
  }
}
