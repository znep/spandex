package com.socrata.spandex.http

// import com.socrata.spandex.common.ElasticsearchServer
import org.scalatest.{BeforeAndAfterAll, MustMatchers, FunSuiteLike}

class SpandexSpec extends FunSuiteLike with MustMatchers with BeforeAndAfterAll {
  val localMasterPort = 9212
  // val esMaster = new ElasticsearchServer(localMasterPort, true)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // esMaster.start()
  }

  override protected def afterAll(): Unit = {
    // esMaster.stop()
    super.afterAll()
  }

  test("spawn") {
    val thread = new Thread {
      override def run(): Unit = {
        // TODO: fix: this exception test requires manual observation
        a[InterruptedException] should be thrownBy {
          Spandex.main(Array.empty)
        }
      }
    }
    thread.start()
    while (!Spandex.ready) {
      Thread.sleep(1000) // scalastyle:ignore magic.number
    }
    thread.interrupt()
  }
}
