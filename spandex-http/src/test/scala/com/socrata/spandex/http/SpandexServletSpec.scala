package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import com.socrata.spandex.common.client.{ColumnMap, SpandexElasticSearchClient, TestESClient}
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatest.FunSuiteLike
import org.scalatra.servlet.ScalatraListener
import org.scalatra.test.scalatest._

class TestSpandexServlet(val conf: SpandexConfig) extends SpandexServletLike {
  override def client: SpandexElasticSearchClient = new TestESClient(conf.es)
  override def index: String = conf.es.index
}

// For more on Specs2, see http://etorreborre.github.com/specs2/guide/org.specs2.guide.QuickStart.html
class SpandexServletSpec extends ScalatraSuite with FunSuiteLike with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es, false)
  val pathRoot = "/"

  override def beforeAll(): Unit = {
    val context = new WebAppContext
    context.setContextPath(pathRoot)
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[TestSpandexServlet], pathRoot)

    server.setHandler(context)
    server.start()

    bootstrapData()
  }
  override def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    server.stop()
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
    }
  }

  private[this] val suggest = "/suggest"
  private[this] val dsid = datasets(0)
  private[this] val copyid = 2
  private[this] val copynum = copyid.toString
  private[this] val colsysid = 3
  private[this] val colid = s"col$colsysid"
  test("suggest") {
    client.putColumnMap(new ColumnMap(dsid, copyid, colsysid, colid))

    get(s"$suggest/$dsid/$copynum/$colid/dat") {
      status should equal(HttpStatus.SC_OK)
      body shouldNot include("data column 3 row 0")
      body should include("data column 3 row 1")
      body should include("data column 3 row 2")
      body should include("data column 3 row 3")
      body should include("data column 3 row 4")
      body should include("data column 3 row 5")
      body shouldNot include("data column 3 row 6")
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
}
