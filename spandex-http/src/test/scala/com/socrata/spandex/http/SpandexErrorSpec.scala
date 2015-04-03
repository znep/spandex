package com.socrata.spandex.http

import com.rojoma.json.v3.util.JsonUtil
import org.scalatest.{FunSuiteLike, Matchers}

class SpandexErrorSpec extends FunSuiteLike with Matchers {
  val message = "this is a test"
  val messageJson = "\"message\":\"%s\"".format(message)
  val entity = "object"
  val source = "spandex-http"
  val sourceJson = "\"source\":\"%s\"".format(source)

  test("json encode - full") {
    val e = SpandexError(message, Some(entity), source)
    val js = JsonUtil.renderJson(e)
    js should include(messageJson)
    js should include("\"entity\":\"%s\"".format(entity))
    js should include(sourceJson)
  }

  test("json encode - brief") {
    val e = SpandexError(message)
    val js = JsonUtil.renderJson(e)
    js should include(messageJson)
    js shouldNot include("\"entity\"")
    js shouldNot include("\"underlying\"")
    js should include(sourceJson)
  }
}
