package com.socrata.spandex

import javax.servlet.http.{HttpServletResponse => HttpStatus}
import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike {
  override def beforeAll: Unit = {
    super.beforeAll
    addServlet(new SpandexServlet(), "/*")
  }

  override def afterAll: Unit = {
    super.afterAll
  }

  test("get of index page") {
    get("/") {
      status should equal (HttpStatus.SC_OK)
      val contentType: String = header.getOrElse("Content-Type", "")
      contentType should include ("text/html")
      body should include ("spandex")
    }
  }

  test("get of non-existent page") {
    get("/goodbye-world"){
      status should equal (HttpStatus.SC_NOT_FOUND)
    }
  }

  test("get health status page") {
    get("/health"){
      status should equal (HttpStatus.SC_OK)
      body should include ("{\"health\":\"alive\"}")
    }
  }

  test("addindex new customer"){
    get("/addindex/chicago"){
      status should equal (HttpStatus.SC_OK)
      body should include ("{\"acknowledged\":true}")
    }
  }

  test("addcol new column"){
    post("/addcol/chicago/qnmj-8ku6","crimeType"){
      status should equal (HttpStatus.SC_OK)
      body should include ("{\"acknowledged\":true}")
    }
  }

  test("version upsert"){
    post(
      "/version/chicago/qnmj-8ku6",
      "{\"_id\": \"1\", \"crimeType\": \"NARCOTICS\"}\n" +
        "{\"_id\": \"2\", \"crimeType\": \"PUBLIC INDECENCY\"}"
    ){
      status should equal (HttpStatus.SC_OK)
      body should include ("{\"create\":{\"_index\":\"chicago\",\"_type\":\"qnmj-8ku6\"," +
        "\"_id\":\"1\",\"_version\":1,\"status\":201}}")
      body should include ("{\"create\":{\"_index\":\"chicago\",\"_type\":\"qnmj-8ku6\"," +
        "\"_id\":\"2\",\"_version\":1,\"status\":201}}")
    }
  }

  test("resync"){
    post(
      "/resync/chicago/gnmj-8ku6",
      ""
    ){
      status should equal (HttpStatus.SC_OK)
      body should include ("{\"acknowledged\":true}")
    }
  }

  test("suggest"){
    get("/suggest/chicago/qnmj-8ku6/crimeType/nar"){
      status should equal (HttpStatus.SC_OK)
      body should include ("NARCOTICS")
    }
  }
}
