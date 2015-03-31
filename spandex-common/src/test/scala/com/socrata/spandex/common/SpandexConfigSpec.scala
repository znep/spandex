package com.socrata.spandex.common

import org.elasticsearch.common.unit.Fuzziness
import org.scalatest.{FunSuiteLike, Matchers}

class SpandexConfigSpec extends FunSuiteLike with Matchers {
  test("spandex config has these required values") {
    val conf = new SpandexConfig
    Some(conf.spandexPort) should be ('defined)
    Some(conf.suggestFuzziness) should be ('defined)
    Some(conf.suggestSize) should be ('defined)
  }

  test("elasticsearch config has these required values") {
    val conf = new SpandexConfig().es
    Some(conf.host) should be ('defined)
    Some(conf.port) should be ('defined)
    Some(conf.clusterName) should be ('defined)
    Some(conf.index) should be ('defined)
    Some(conf.fieldValueMapping) should be ('defined)
    Some(conf.columnMapMapping) should be ('defined)
    Some(conf.datasetCopyMapping) should be ('defined)
    Some(conf.dataCopyBatchSize) should be ('defined)
    Some(conf.dataCopyTimeout) should be ('defined)
  }

  test("config fuzziness acceptable values") {
    Fuzziness.build("AUTO") should be (Fuzziness.AUTO)
    Fuzziness.build("0").asInt should be (Fuzziness.ZERO.asInt)
    Fuzziness.build("1").asInt should be (Fuzziness.ONE.asInt)
    Fuzziness.build("2").asInt should be (Fuzziness.TWO.asInt)
  }
}
