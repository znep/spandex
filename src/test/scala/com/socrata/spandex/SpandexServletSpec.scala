package com.socrata.spandex

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._
import wabisabi.{Client => ElasticsearchClient}

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike {
  val acknowledged = "\"acknowledged\":true"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // TODO: config value for elasticsearch cluster url
    val elasticsearch = new ElasticsearchClient("http://eel:9200")
    addServlet(new SpandexServlet(elasticsearch), "/*")
  }

  override def afterAll(): Unit = {
    super.afterAll()
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
      body should include ("\"status\":\"green\"")
    }
  }

  test("get all spandex mappings") {
    get("/mapping") {
      status should equal (HttpStatus.SC_OK)
      body should include ("spandex")
    }
  }

  test("add new dataset"){
    get("/add/qnmj-8ku6"){
      status should equal (HttpStatus.SC_OK)
      body should include (acknowledged)
    }
    get("/mapping") {
      status should equal (HttpStatus.SC_OK)
      body should include ("qnmj-8ku6")
    }
  }

  test("add new column"){
    get("/add/qnmj-8ku6/crimeType"){
      status should equal (HttpStatus.SC_OK)
      body should include (acknowledged)
    }
    get("/mapping") {
      status should equal (HttpStatus.SC_OK)
      body should include ("crimeType")
    }
  }

  test("version insert") {
    post("/ver/qnmj-8ku6",
    """
      |{"index": {"_id": "1"} }
      |{"crimeType": "NARCOTICS"}
      |{"index": {"_id": "2"} }
      |{"crimeType": "PUBLIC INDECENCY"}
    """.stripMargin
    ) {
      status should equal(HttpStatus.SC_OK)
      body should include("\"errors\":false")
      body should include("\"index\":{\"_index\":\"spandex\",\"_type\":\"qnmj-8ku6\",\"_id\":\"1\"")
      body should include("\"index\":{\"_index\":\"spandex\",\"_type\":\"qnmj-8ku6\",\"_id\":\"2\"")
    }
  }

  test("resync"){
    get("/syn/gnmj-8ku6"){
      status should equal (HttpStatus.SC_OK)
      body should include ("\"failed\":0}")
    }
  }

  test("suggest"){
    get("/suggest/qnmj-8ku6/crimeType/nar"){
      status should equal (HttpStatus.SC_OK)
      body should include ("NARCOTICS")
      body shouldNot include ("PUBLIC INDECENCY")
    }
  }
}
