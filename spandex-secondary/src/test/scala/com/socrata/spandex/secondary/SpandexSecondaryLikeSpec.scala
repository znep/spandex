package com.socrata.spandex.secondary

import com.socrata.spandex.common._
import com.socrata.spandex.common.client.{DatasetCopy, TestESClient}
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
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    secondary.dropDataset(datasets(0), None)

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("drop copy") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should be ('defined)
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (3)

    secondary.dropCopy(datasets(0), 2, None)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should not be 'defined
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (0)
  }
}
