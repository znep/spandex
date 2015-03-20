package com.socrata.spandex.secondary

import com.socrata.spandex.common._
import com.socrata.spandex.common.client.TestESClient
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSuiteLike}

class TestSpandexSecondary(config: ElasticSearchConfig) extends SpandexSecondaryLike {
  val client = new TestESClient(config)
  val index = config.index
}

// scalastyle:off
class SpandexSecondaryLikeSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterEach {
  lazy val config = new SpandexConfig
  lazy val secondary = new TestSpandexSecondary(config.es)

  def client = secondary.client

  override def beforeEach(): Unit = bootstrapData()

  override def afterEach(): Unit = removeBootstrapData()

  test("drop dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (30)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)

    secondary.dropDataset(datasets(0), None)

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)
  }

  test("drop copy") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    secondary.dropCopy(datasets(0), 2, None)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
  }
}
