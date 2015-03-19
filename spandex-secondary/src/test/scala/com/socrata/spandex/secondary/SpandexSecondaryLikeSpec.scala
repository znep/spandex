package com.socrata.spandex.secondary

import com.socrata.spandex.common.{TestESData, ElasticSearchConfig, SpandexConfig, TestESClient}
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSuiteLike}

class TestSpandexSecondary(config: ElasticSearchConfig) extends SpandexSecondaryLike {
  val client = new TestESClient(config)
  val index = config.index
}

class SpandexSecondaryLikeSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterEach {
  lazy val config = new SpandexConfig
  lazy val secondary = new TestSpandexSecondary(config.es)

  def client = secondary.client

  override def beforeEach(): Unit = bootstrapData()

  override def afterEach(): Unit = removeBootstrapData()

  test("drop dataset") {
    client.searchByDataset(datasets(0)).getHits.totalHits should be(30)
    client.searchByDataset(datasets(1)).getHits.totalHits should be(30)

    secondary.dropDataset(datasets(0), None)

    client.searchByDataset(datasets(0)).getHits.totalHits should be(0)
    client.searchByDataset(datasets(1)).getHits.totalHits should be(30)
  }

  test("drop copy") {
    client.searchByCopyId(datasets(0), 1).getHits.totalHits should be(15)
    client.searchByCopyId(datasets(0), 2).getHits.totalHits should be(15)
    client.searchByCopyId(datasets(1), 1).getHits.totalHits should be(15)
    client.searchByCopyId(datasets(1), 2).getHits.totalHits should be(15)

    secondary.dropCopy(datasets(0), 2, None)

    client.searchByCopyId(datasets(0), 1).getHits.totalHits should be(15)
    client.searchByCopyId(datasets(0), 2).getHits.totalHits should be(0)
    client.searchByCopyId(datasets(1), 1).getHits.totalHits should be(15)
    client.searchByCopyId(datasets(1), 2).getHits.totalHits should be(15)
  }
}