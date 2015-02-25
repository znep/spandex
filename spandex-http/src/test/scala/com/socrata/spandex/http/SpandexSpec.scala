package com.socrata.spandex.http

import org.scalatest.{MustMatchers, FunSuiteLike}

class SpandexSpec extends FunSuiteLike with MustMatchers {
  test("spawn") {
    val thread = new Thread {
      override def run(): Unit = {
        Spandex.main(Array.empty)
      }
    }
    thread.start()
  }
}
