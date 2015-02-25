package com.socrata.spandex.common

import org.scalatest.{Matchers, FunSuiteLike}

class SpandexBootstrapSpec extends FunSuiteLike with Matchers{
  test("bootstrap ensure index") {
    SpandexBootstrap.ensureIndex(new SpandexConfig)
  }
}
