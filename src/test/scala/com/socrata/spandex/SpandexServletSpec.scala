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

  test("get of non-existent page") {
    get("/goodbye-world"){
      status should equal (HttpStatus.NotFound)
    }
  }

  test("addindex new customer"){
    get("/addindex/chicago"){
      status should equal (HttpStatus.Success)
    }
  }

  test("addcol new column"){
    post("/addcol/chicago/qnmj-8ku6","crimeType"){
      status should equal (HttpStatus.Success)
    }
  }

  test("version upsert"){
    post(
      "/version/chicago/qnmj-8ku6",
      "{\"crimeType\": \"NARCOTICS\"}"
    ){
      status should equal (HttpStatus.Success)
    }
  }

  test("resync"){
    post(
      "/resync/chicago/gnmj-8ku6",
      ""
    ){
      status should equal (HttpStatus.Success)
    }
  }

  test("suggest"){
    get("/suggest/chicago/qnmj-8ku6/crimeType/nar"){
      status should equal (HttpStatus.Success)
    }
  }
}

object HttpStatus {
  val Success = 200
  val NotFound = 404
}
