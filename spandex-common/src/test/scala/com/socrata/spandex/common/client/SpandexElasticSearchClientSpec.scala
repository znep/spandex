package com.socrata.spandex.common.client

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.{TestESData, SpandexConfig}
import org.elasticsearch.rest.RestStatus
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, FunSuiteLike}

// scalastyle:off
class SpandexElasticSearchClientSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)

  override def afterAll(): Unit = client.close()

  override def beforeEach(): Unit = {
    bootstrapData()
    client.deleteAllDatasetCopies()
  }
  override def afterEach(): Unit = removeBootstrapData()

  test("Delete field values by dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (30)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)

    val response = client.deleteFieldValuesByDataset(datasets(0))
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)
  }

  test("Delete field values by copy number") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    val response = client.deleteFieldValuesByCopyNumber(datasets(0), 2)
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
  }

  test("Delete field values by column") {
    client.searchFieldValuesByColumnId(datasets(0), 1, columns(0)).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(0)).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(1)).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, columns(0)).totalHits should be (5)

    val response = client.deleteFieldValuesByColumnId(datasets(0), 2, columns(0))
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchFieldValuesByColumnId(datasets(0), 1, columns(0)).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(0)).totalHits should be (0)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(1)).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, columns(0)).totalHits should be (5)
  }

  test("Delete field values by row") {
    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)

    val response = client.deleteFieldValuesByRowId(datasets(0), 2, 1)
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (0)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)
  }

  test("Put, get and search dataset copies") {
    client.getDatasetCopy(datasets(0), 1) should not be 'defined
    client.getDatasetCopy(datasets(0), 2) should not be 'defined
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)

    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published)
    Thread.sleep(1000) // Account for ES indexing delay

    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 1).get.datasetId should be (datasets(0))
    client.getDatasetCopy(datasets(0), 1).get.copyNumber should be (1)
    client.getDatasetCopy(datasets(0), 1).get.version should be (50)
    client.getDatasetCopy(datasets(0), 1).get.stage should be (LifecycleStage.Unpublished)

    client.getDatasetCopy(datasets(0), 2) should be ('defined)
    client.getDatasetCopy(datasets(0), 2).get.datasetId should be (datasets(0))
    client.getDatasetCopy(datasets(0), 2).get.copyNumber should be (2)
    client.getDatasetCopy(datasets(0), 2).get.version should be (100)
    client.getDatasetCopy(datasets(0), 2).get.stage should be (LifecycleStage.Published)

    client.searchCopiesByDataset(datasets(0)).totalHits should be (2)
  }

  test("Get latest copy of dataset") {
    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published)
    client.putDatasetCopy(datasets(1), 3, 150L, LifecycleStage.Unpublished)
    Thread.sleep(1000) // Account for ES indexing delay

    client.getLatestCopyForDataset(datasets(0)) should be ('defined)
    client.getLatestCopyForDataset(datasets(0)).get.copyNumber should be (2)
    client.getLatestCopyForDataset(datasets(1)) should be ('defined)
    client.getLatestCopyForDataset(datasets(1)).get.copyNumber should be (3)
    client.getLatestCopyForDataset("foo") should not be ('defined)
  }

  test("Update dataset copy version") {
    client.putDatasetCopy(datasets(1), 1, 2, LifecycleStage.Unpublished)
    Thread.sleep(1000) // Account for ES indexing delay

    val current = client.getDatasetCopy(datasets(1), 1)
    current should be ('defined)
    current.get.version should be (2)
    current.get.stage should be (LifecycleStage.Unpublished)

    client.updateDatasetCopyVersion(current.get.updateCopy(5, LifecycleStage.Published))
    Thread.sleep(1000) // Account for ES indexing delay

    client.getDatasetCopy(datasets(1), 1) should be ('defined)
    client.getDatasetCopy(datasets(1), 1).get.version should be (5)
    client.getDatasetCopy(datasets(1), 1).get.stage should be (LifecycleStage.Published)
  }

  test("Delete dataset copy by copy number") {
    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published)
    Thread.sleep(1000) // Account for ES indexing delay

    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should be ('defined)

    client.deleteDatasetCopy(datasets(0), 2)
    Thread.sleep(1000) // Account for ES indexing delay

    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should not be ('defined)
  }
}
