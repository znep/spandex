package com.socrata.spandex.common.client

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.{SpandexConfig, TestESData}
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.common.unit.Fuzziness
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
    client.sendBulkRequest(inserts, refresh = true)

    toInsert.foreach { fv =>
      client.getFieldValue(fv) should be (Some(fv))
    }

    val toUpdate = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "Mexican axolotl"),
      FieldValue("alpha.1337", 1, 22, 32, "Enrique"))

    val updates = toUpdate.map(client.getUpdateRequest)
    client.sendBulkRequest(updates, refresh = true)

    client.getFieldValue(toInsert(0)).get should be (toUpdate(0))
    client.getFieldValue(toInsert(1)).get should be (toInsert(1))
    client.getFieldValue(toInsert(2)).get should be (toUpdate(1))
  }

  test("Don't send empty bulk requests to Elastic Search") {
    val empty = Seq.empty[IndexRequestBuilder]
    // BulkRequest.validate throws if given an empty bulk request
    client.sendBulkRequest(empty, refresh = true)
  }

  test("Delete field values by dataset") {
    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)

    client.deleteFieldValuesByDataset(datasets(0))

    client.searchFieldValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchFieldValuesByDataset(datasets(1)).totalHits should be (45)
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

  test("Copy field values from one dataset copy to another") {
    val from = DatasetCopy("copy-test", 1, 2, LifecycleStage.Published)
    val to   = DatasetCopy("copy-test", 2, 3, LifecycleStage.Unpublished)

    val toCopy = for {
                   col <- 1 to 10
                   row <- 1 to 10
                 } yield FieldValue(from.datasetId, from.copyNumber, col, row, s"$col|$row")

    val inserts = toCopy.map(client.getIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (0)

    client.copyFieldValues(from, to, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (100)
  }

  test("Put, get and delete column map") {
    client.getColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
    client.getColumnMap(datasets(0), 1, "col2-2222") should not be 'defined

    val colMap = ColumnMap(datasets(0), 1, 1, "col1-1111")
    client.putColumnMap(colMap, refresh = true)

    val colMap1 = client.getColumnMap(datasets(0), 1, "col1-1111")
    colMap1 should be (Some(colMap))
    val colMap2 = client.getColumnMap(datasets(0), 1, "col2-2222")
    colMap2 should not be 'defined

    client.deleteColumnMap(colMap.datasetId, colMap.copyNumber, colMap.userColumnId)

    client.getColumnMap(datasets(0), 1, "col1-1111") should not be 'defined
  }

  test("Delete column map by dataset") {
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    client.deleteColumnMapsByDataset(datasets(0))

    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("Delete column map by copy number") {
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)

    client.deleteColumnMapsByCopyNumber(datasets(0), 1)

    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
  }

  test("Put, get, search, delete dataset copies") {
    val dsCopies = client.searchCopiesByDataset(datasets(0))
    dsCopies.totalHits should be (3)
    dsCopies.thisPage.sortBy(_.copyNumber) should be (copies(datasets(0)))

    client.getDatasetCopy(datasets(0), 3) should be (Some(copies(datasets(0))(2)))
    client.getDatasetCopy(datasets(0), 4) should not be 'defined

    client.putDatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished, refresh = true)
    client.getDatasetCopy(datasets(0), 4) should be
      (Some(DatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished)))

    client.deleteDatasetCopy(datasets(0), 4)
    client.getDatasetCopy(datasets(0), 4) should not be 'defined
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)

    client.deleteDatasetCopiesByDataset(datasets(0))
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
  }

  test("Get latest copy of dataset (published or all)") {
    client.getLatestCopyForDataset(datasets(0)) should be ('defined)
    client.getLatestCopyForDataset(datasets(0)).get.copyNumber should be (3)
    client.getLatestCopyForDataset(datasets(0), publishedOnly = false) should be ('defined)
    client.getLatestCopyForDataset(datasets(0), publishedOnly = false).get.copyNumber should be (3)
    client.getLatestCopyForDataset(datasets(0), publishedOnly = true) should be ('defined)
    client.getLatestCopyForDataset(datasets(0), publishedOnly = true).get.copyNumber should be (2)

    client.getLatestCopyForDataset("foo") should not be 'defined
  }

  test("Update dataset copy version") {
    client.putDatasetCopy(datasets(1), 1, 2, LifecycleStage.Unpublished, refresh = true)

    val current = client.getDatasetCopy(datasets(1), 1)
    current should be ('defined)
    current.get.version should be (2)
    current.get.stage should be (LifecycleStage.Unpublished)

    client.updateDatasetCopyVersion(current.get.updateCopy(5, LifecycleStage.Published), refresh = true)

    client.getDatasetCopy(datasets(1), 1) should be ('defined)
    client.getDatasetCopy(datasets(1), 1).get.version should be (5)
    client.getDatasetCopy(datasets(1), 1).get.stage should be (LifecycleStage.Published)
  }

  test("Delete dataset copy by copy number") {
    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished, refresh = true)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published, refresh = true)

    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should be ('defined)

    client.deleteDatasetCopy(datasets(0), 2)

    client.getDatasetCopy(datasets(0), 1) should be ('defined)
    client.getDatasetCopy(datasets(0), 2) should not be ('defined)
  }

  test("suggest is case insensitive") {
    val column = ColumnMap(datasets(0), 1, 1, "col1-1111")
    val fool = FieldValue(datasets(0), 1, 1, 42L, "fool")
    val food = FieldValue(datasets(0), 1, 1, 43L, "FOOD")

    client.indexFieldValue(fool, true)
    client.indexFieldValue(food, true)

    val suggestions = client.getSuggestions(column, "foo", Fuzziness.AUTO, 10)

    suggestions.size() should be(1)
    suggestions.toString should include("\"options\" : [")
    suggestions.toString should include(food.value)
    suggestions.toString should include(fool.value)

    val suggestionsUpper = client.getSuggestions(column, "FOO", Fuzziness.AUTO, 10)

    suggestionsUpper.size() should be(1)
    suggestionsUpper.toString should include("\"options\" : [")
    suggestionsUpper.toString should include(food.value)
    suggestionsUpper.toString should include(fool.value)
  }

  test("samples") {
    val column = ColumnMap(datasets(0), 1, 1, "col1-1111")

    val samples = client.getSamples(column, 10)

    samples.totalHits should be(5)
    samples.thisPage should contain(FieldValue(datasets(0), 1, 1, 1, "data column 1 row 1"))
    samples.thisPage should contain(FieldValue(datasets(0), 1, 1, 2, "data column 1 row 2"))
    samples.thisPage should contain(FieldValue(datasets(0), 1, 1, 3, "data column 1 row 3"))
    samples.thisPage should contain(FieldValue(datasets(0), 1, 1, 4, "data column 1 row 4"))
    samples.thisPage should contain(FieldValue(datasets(0), 1, 1, 5, "data column 1 row 5"))
  }
}
