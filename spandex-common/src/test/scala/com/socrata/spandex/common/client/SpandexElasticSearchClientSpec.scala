package com.socrata.spandex.common.client

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.{SpandexConfig, TestESData}
import org.elasticsearch.rest.RestStatus
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

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

  test("Insert, update, get field values") {
    val toInsert = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "axolotl"),
      FieldValue("alpha.1337", 1, 21, 32, "amphibious"),
      FieldValue("alpha.1337", 1, 22, 32, "Henry"))

    toInsert.foreach { fv =>
      client.getFieldValue(fv) should not be 'defined
    }

    val inserts = toInsert.map(client.getIndexRequest)
    client.sendBulkRequest(inserts)

    toInsert.foreach { fv =>
      client.getFieldValue(fv) should be (Some(fv))
    }

    val toUpdate = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "Mexican axolotl"),
      FieldValue("alpha.1337", 1, 22, 32, "Enrique"))

    val updates = toUpdate.map(client.getUpdateRequest)
    client.sendBulkRequest(updates)

    client.getFieldValue(toInsert(0)).get should be (toUpdate(0))
    client.getFieldValue(toInsert(1)).get should be (toInsert(1))
    client.getFieldValue(toInsert(2)).get should be (toUpdate(1))
  }

  test("Delete field values by dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (30)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)

    client.deleteFieldValuesByDataset(datasets(0))

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (30)
  }

  test("Delete field values by copy number") {
    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    client.deleteFieldValuesByCopyNumber(datasets(0), 2)

    client.searchFieldValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
  }

  test("Delete field values by column") {
    client.searchFieldValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)

    client.deleteFieldValuesByColumnId(datasets(0), 2, 1)

    client.searchFieldValuesByColumnId(datasets(0), 1, 1).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(0), 2, 1).totalHits should be (0)
    client.searchFieldValuesByColumnId(datasets(0), 2, 2).totalHits should be (5)
    client.searchFieldValuesByColumnId(datasets(1), 2, 1).totalHits should be (5)
  }

  test("Delete field values by row") {
    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)

    client.deleteFieldValuesByRowId(datasets(0), 2, 1)

    client.searchFieldValuesByRowId(datasets(0), 2, 1).totalHits should be (0)
    client.searchFieldValuesByRowId(datasets(0), 2, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(0), 1, 1).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), 2, 1).totalHits should be (3)
  }

  test("Put, get and delete column map") {
    client.getColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
    client.getColumnMap(datasets(0), 1, "col2-2222") should not be 'defined

    val colMap = ColumnMap(datasets(0), 1, 1, "col1-1111")
    client.putColumnMap(colMap)
    Thread.sleep(1000) // Account for ES indexing delay

    val colMap1 = client.getColumnMap(datasets(0), 1, "col1-1111")
    colMap1 should be (Some(colMap))
    val colMap2 = client.getColumnMap(datasets(0), 1, "col2-2222")
    colMap2 should not be 'defined

    client.deleteColumnMap(colMap.datasetId, colMap.copyNumber, colMap.userColumnId)

    client.getColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
  }

  test("Delete column map by dataset") {
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (6)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (6)

    val response = client.deleteColumnMapsByDataset(datasets(0))
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (6)
  }

  test("Delete column map by copy number") {
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)

    val response = client.deleteColumnMapsByCopyNumber(datasets(0), 1)
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
  }

  test("Put, get and search dataset copies") {
    client.deleteDatasetCopiesByDataset(datasets(0))
    Thread.sleep(1000) // Account for ES indexing delay

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
