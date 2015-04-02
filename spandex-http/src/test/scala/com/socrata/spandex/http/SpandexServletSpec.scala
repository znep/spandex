package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import com.socrata.spandex.common.client.TestESClient
import com.socrata.spandex.http.SpandexResult.Fields._
import org.scalatest.FunSuiteLike
import org.scalatra.test.scalatest._

// scalastyle:off magic.number
// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es, false)

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
    get("/") {
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

  private[this] val dsid = datasets(0)
  private[this] val copy = copies(dsid)(1)
  private[this] val copynum = copy.copyNumber
  private[this] val col = columns(dsid, copy)(2)
  private[this] val colsysid = col.systemColumnId
  private[this] val colid = col.userColumnId
  private[this] val textPrefix = "dat"
  test("suggest - some hits") {
    get(s"$routeSuggest/$dsid/$copynum/$colid/$textPrefix") {
      status should equal(HttpStatus.SC_OK)
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      body should include(optionsJson)
      body shouldNot include(makeRowData(colsysid, 0))
      body should include(makeRowData(colsysid, 1))
      body should include(makeRowData(colsysid, 2))
      body should include(makeRowData(colsysid, 3))
      body should include(makeRowData(colsysid, 4))
      body should include(makeRowData(colsysid, 5))
      body shouldNot include(makeRowData(colsysid, 6))
    }
  }

  test("suggest - param size") {
    val text = "data+column+3"
    get(s"$routeSuggest/$dsid/$copynum/$colid/$text", (paramSize, "10")) {
      status should equal(HttpStatus.SC_OK)
      body should include(makeRowData(colsysid, 1))
      body should include(makeRowData(colsysid, 2))
      body should include(makeRowData(colsysid, 3))
      body should include(makeRowData(colsysid, 4))
      body should include(makeRowData(colsysid, 5))
    }
    get(s"$routeSuggest/$dsid/$copynum/$colid/$text", (paramSize, "1")) {
      status should equal(HttpStatus.SC_OK)
      body should include(makeRowData(colsysid, 1))
      body shouldNot include(makeRowData(colsysid, 2))
      body shouldNot include(makeRowData(colsysid, 3))
      body shouldNot include(makeRowData(colsysid, 4))
      body shouldNot include(makeRowData(colsysid, 5))
    }
  }

  test("suggest - param fuzz(iness)") {
    val text = "drat"
    get(s"$routeSuggest/$dsid/$copynum/$colid/$text", (paramFuzz, "0")) {
      status should equal(HttpStatus.SC_OK)
      body should include(optionsEmptyJson)
    }
    get(s"$routeSuggest/$dsid/$copynum/$colid/$text", (paramFuzz, "2")) {
      status should equal(HttpStatus.SC_OK)
      body should include(makeRowData(colsysid, 1))
    }
  }

  test("suggest - no hits") {
    get(s"$routeSuggest/$dsid/$copynum/$colid/nar") {
      status should equal(HttpStatus.SC_OK)
      body should include(optionsEmptyJson)
    }
  }

  test("suggest - non-numeric copy number should return 400") {
    get(s"$routeSuggest/$dsid/giraffe/$colid/$textPrefix") {
      status should equal (HttpStatus.SC_BAD_REQUEST)
      body should be ("Copy number must be numeric")
    }
  }

  test("suggest - non-existent column should return 400") {
    get(s"$routeSuggest/$dsid/$copynum/giraffe/$textPrefix") {
      status should equal (HttpStatus.SC_BAD_REQUEST)
      body should be (s"column 'giraffe' not found")
    }
  }

  test("suggest - samples") {
    get(s"$routeSuggest/$dsid/$copynum/$colid") {
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      status should equal(HttpStatus.SC_OK)
      body should include(optionsJson)
      body should include(makeRowData(colsysid, 2))
    }
  }

  test("suggest without required params should return 404") {
    get(s"$routeSuggest/$dsid/$copynum") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$routeSuggest/$dsid") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$routeSuggest") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
  }

  ignore("sample") {
    get(s"$routeSample/$dsid/$copynum/$colid") {
      val contentType: String = header.getOrElse(ContentTypeHeader, "")
      contentType should include(ContentTypeJson)
      status should equal(HttpStatus.SC_OK)
      body should include(optionsJson)
      body should include(makeRowData(colsysid, 2))
    }
  }

  ignore("sample without required params should return 404") {
    get(s"$routeSample/$dsid/$copynum") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$routeSample/$dsid") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
    get(s"$routeSample") {
      status should equal(HttpStatus.SC_NOT_FOUND)
    }
  }
}
