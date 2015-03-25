package com.socrata.spandex.http

import org.scalatest.{BeforeAndAfterAll, MustMatchers, FunSuiteLike}

class SpandexSpec extends FunSuiteLike with MustMatchers with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
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
