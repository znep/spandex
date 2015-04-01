package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import com.socrata.spandex.common.client.TestESClient
import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es, false)
  val pathRoot = "/"
  val options: String = "\"options\" :" // expecting some options
  val optionsEmpty: String = "\"options\" : []" // expecting empty result set

  addServlet(new SpandexServlet(config, client), "/*")

  private def getRandomPort: Int = 51200 + (util.Random.nextInt % 100)
  override def localPort: Option[Int] = Some(getRandomPort)

  override def beforeAll(): Unit = {
    start()
    removeBootstrapData()
    bootstrapData()
  }
  override def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    stop()
  }

  test("get of index page") {
    get(pathRoot) {
      status should equal(HttpStatus.SC_OK)
      val contentType: String = header.getOrElse("Content-Type", "")
      contentType should include("text/html")
      body should include("Hello, spandex")
    }
  }

  test("get of non-existent page") {
    get("/goodbye-world") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
  }

  test("get health status page") {
    get("/health") {
      status should equal(HttpStatus.SC_OK)
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
    }
  }

  test("get version") {
    get("/version") {
      status should equal(HttpStatus.SC_OK)
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      body should include(""""name":"spandex-http"""")
      body should include(""""version"""")
      body should include(""""revision"""")
      body should include(""""buildTime"""")
    }
  }

  private[this] val suggest = "/suggest"
  private[this] val dsid = datasets(0)
  private[this] val copyid = 2
  private[this] val copynum = copyid.toString
  private[this] val colsysid = 3
  private[this] val colid = s"col$colsysid"
  test("suggest - some hits") {
    get(s"$suggest/$dsid/$copynum/$colid/dat") {
      status should equal(HttpStatus.SC_OK)
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      body should include(options)
      body shouldNot include("data column 3 row 0")
      body should include("data column 3 row 1")
      body should include("data column 3 row 2")
      body should include("data column 3 row 3")
      body should include("data column 3 row 4")
      body should include("data column 3 row 5")
      body shouldNot include("data column 3 row 6")
    }
  }

  test("suggest - param size") {
    get(s"$suggest/$dsid/$copynum/$colid/data+column+3", ("size", "10")) {
      status should equal(HttpStatus.SC_OK)
      body should include("data column 3 row 1")
      body should include("data column 3 row 2")
      body should include("data column 3 row 3")
      body should include("data column 3 row 4")
      body should include("data column 3 row 5")
    }
    get(s"$suggest/$dsid/$copynum/$colid/data+column+3", ("size", "1")) {
      status should equal(HttpStatus.SC_OK)
      body should include("data column 3 row 1")
      body shouldNot include("data column 3 row 2")
      body shouldNot include("data column 3 row 3")
      body shouldNot include("data column 3 row 4")
      body shouldNot include("data column 3 row 5")
    }
  }

  test("suggest - param fuzz(iness)") {
    get(s"$suggest/$dsid/$copynum/$colid/drat", ("fuzz", "0")) {
      status should equal(HttpStatus.SC_OK)
      body should include(optionsEmpty)
    }
    get(s"$suggest/$dsid/$copynum/$colid/drat", ("fuzz", "2")) {
      status should equal(HttpStatus.SC_OK)
      body should include("data column 3 row 1")
    }
  }

  test("suggest - no hits") {
    get(s"$suggest/$dsid/$copynum/$colid/nar") {
      status should equal(HttpStatus.SC_OK)
      body should include(optionsEmpty)
    }
  }

  test("suggest - non-numeric copy number should return 400") {
    get(s"$suggest/$dsid/giraffe/$colid/dat") {
      status should equal (HttpStatus.SC_BAD_REQUEST)
      body should be ("Copy number must be numeric")
    }
  }

  test("suggest - non-existent column should return 400") {
    get(s"$suggest/$dsid/$copynum/giraffe/dat") {
      status should equal (HttpStatus.SC_BAD_REQUEST)
      body should be (s"column 'giraffe' not found")
    }
  }

  test("suggest without required params should return 404") {
    get(s"$suggest/$dsid/$copynum/$colid/") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$suggest/$dsid/$copynum/") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$suggest/$dsid/") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$suggest/") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
  }

  // TODO: score?
  private[this] val sample = "/sample"
  test("sample") {
    get(s"$sample/$dsid/$copynum/$colid") {
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      status should equal(HttpStatus.SC_OK)
      body should include(options)
      body should include("\"text\" : \"data column 3 row 1\"")
    }
  }

  test("sample without required params should return 404") {
    get(s"$sample/$dsid/$copynum") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$sample/$dsid") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$sample") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
  }
}
