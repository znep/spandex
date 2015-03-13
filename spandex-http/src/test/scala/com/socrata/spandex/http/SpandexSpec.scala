package com.socrata.spandex.http

import org.scalatest.{MustMatchers, FunSuiteLike}

class SpandexSpec extends FunSuiteLike with MustMatchers {
  test("spawn") {
    val thread = new Thread {
      override def run(): Unit = {
        a[java.lang.InterruptedException] should be thrownBy {
          Spandex.main(Array.empty)
        }
      }
    }
    thread.start()
    Thread.sleep(1000) // scalastyle:ignore magic.number
    thread.interrupt()
  }
}
