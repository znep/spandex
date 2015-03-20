package com.socrata.spandex.common

import org.scalatest.{Matchers, FunSuiteLike}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("config has these required values") {
    val conf = new SpandexConfig
    (
      conf.spandexPort,
      conf.es.port,
      conf.es.clusterName,
      conf.clusterTimeout,
      conf.esUrl(0),
      conf.es.index,
      conf.indexSettings,
      conf.es.fieldValueMapping,
      conf.es.datasetCopyMapping,
      conf.indexColumnMapping,
      conf.escTimeout,
      conf.escTimeoutFast,
      conf.bulkBatchSize
    )
  }
}
