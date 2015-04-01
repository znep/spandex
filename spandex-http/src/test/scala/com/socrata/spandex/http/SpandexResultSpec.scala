package com.socrata.spandex.http

import com.rojoma.json.v3.codec.JsonEncode
import com.socrata.spandex.common.client.TestESClient
import com.socrata.spandex.common.{SpandexConfig, TestESData}
import com.socrata.spandex.http.SpandexResult.Fields._
import org.elasticsearch.common.unit.Fuzziness
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, ShouldMatchers}

// scalastyle:off magic.number
class SpandexResultSpec extends FunSuiteLike with ShouldMatchers with TestESData with BeforeAndAfterAll {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)

  override def beforeAll(): Unit = {
    removeBootstrapData()
    bootstrapData()
  }
  override def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  val opt0 = SpandexOption("0", None)
  val opt1 = SpandexOption("1", Some(1))
  val opt2 = SpandexOption("2", Some(2))
  val opt3 = SpandexOption("3", Some(3))
  val opt1String = JsonEncode.toJValue(opt1).toString()
  val opt2String = JsonEncode.toJValue(opt2).toString()
  val opt3String = JsonEncode.toJValue(opt3).toString()

  test("json encode option") {
    val js = JsonEncode.toJValue(opt1).toString()
    js should include(textJson)
    js should include(scoreJson)
  }

  test("json encode option with no score") {
    val js = JsonEncode.toJValue(opt0).toString()
    js should include(textJson)
    js shouldNot include(scoreJson)
  }

  test("json encode result") {
    val js = JsonEncode.toJValue(SpandexResult(Seq(opt2, opt3))).toString()
    js should include(optionsJson)
    js should include(opt2String)
    js should include(opt3String)
  }

  test("json enocde empty result") {
    val js = JsonEncode.toJValue(SpandexResult(Seq.empty)).toString()
    js should include(optionsEmptyJson)
  }

  test("transform from suggest response") {
    val ds = datasets(0)
    val copy = copies(ds)(1)
    val col = columns(ds, copy)(2)
    val suggest = client.getSuggestions(col, "dat", Fuzziness.TWO, 10)
    val js = JsonEncode.toJValue(SpandexResult.fromSuggest(suggest)).toString()
    js should include(optionsJson)
    js should include(rows(col)(0).value)
  }

  test("transform from search response") {
    val ds = datasets(0)
    val copy = copies(ds)(1)
    val col = columns(ds, copy)(2)
    val sample = client.getSamples(col, 10)
    val js = JsonEncode.toJValue(SpandexResult.fromSearch(sample)).toString()
    js should include(optionsJson)
    js should include(rows(col)(0).value)
  }
}
