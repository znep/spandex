package com.socrata.spandex.common

import org.scalatest.{Matchers, FunSuiteLike}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("config has these required values") {
    val conf = new SpandexConfig
    (
      conf.spandexPort,
      conf.es.port,
      conf.es.clusterName,
      conf.es.index,
      conf.es.fieldValueMapping,
      conf.es.datasetCopyMapping,
      conf.es.dataCopyBatchSize,
      conf.es.dataCopyTimeout
    )
  }
}
