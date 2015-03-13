package com.socrata.spandex.common

import org.scalatest.{Matchers, FunSuiteLike}

class SpandexBootstrapSpec extends FunSuiteLike with Matchers {
  val conf = new SpandexConfig
  val localMasterPort = 9210
  val esMaster = new ElasticsearchServer(localMasterPort, true)

  test("start elasticsearch master node and wait for ready") {
    esMaster.start()
    esMaster.waitForReady()
  }

  test("bootstrap ensure index") {
    SpandexBootstrap.ensureIndex(conf, localMasterPort)
  }
  test("stop elasticsearch master node") {
    esMaster.stop()
  }
}
