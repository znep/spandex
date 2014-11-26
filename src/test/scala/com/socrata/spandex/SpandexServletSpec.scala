package com.socrata.spandex

import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike {
  import HttpStatus._
  test("get of index page") {
    get("/") {
      status should equal (HttpStatus.Success)
    }
  }
}

object HttpStatus {
  val Success = 200
}
