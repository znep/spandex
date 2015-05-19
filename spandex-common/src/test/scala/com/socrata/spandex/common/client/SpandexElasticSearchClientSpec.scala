package com.socrata.spandex.common.client

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.soda.server.copy
import com.socrata.spandex.common.client.ResponseExtensions._
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

  def verifyFieldValue(fieldValue: FieldValue): Option[FieldValue] =
    client.client
      .prepareGet(config.es.index, config.es.fieldValueMapping.mappingType, fieldValue.docId)
      .execute.actionGet
      .result[FieldValue]

  test("Insert, update, get field values") {
    val toInsert = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "axolotl"),
      FieldValue("alpha.1337", 1, 21, 32, "amphibious"),
      FieldValue("alpha.1337", 1, 22, 32, "Henry"))

    toInsert.foreach { fv =>
      verifyFieldValue(fv) should not be 'defined
    }

    val inserts = toInsert.map(client.fieldValueIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    toInsert.foreach { fv =>
      verifyFieldValue(fv) should be (Some(fv))
    }

    val toUpdate = Seq(
      FieldValue("alpha.1337", 1, 20, 32, "Mexican axolotl"),
      FieldValue("alpha.1337", 1, 22, 32, "Enrique"))

    val updates = toUpdate.map(client.fieldValueUpdateRequest)
    client.sendBulkRequest(updates, refresh = true)

    verifyFieldValue(toInsert(0)).get should be (toUpdate(0))
    verifyFieldValue(toInsert(1)).get should be (toInsert(1))
    verifyFieldValue(toInsert(2)).get should be (toUpdate(1))
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

    val inserts = toCopy.map(client.fieldValueIndexRequest)
    client.sendBulkRequest(inserts, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (0)

    client.copyFieldValues(from, to, refresh = true)

    client.searchFieldValuesByCopyNumber(from.datasetId, from.copyNumber).totalHits should be (100)
    client.searchFieldValuesByCopyNumber(to.datasetId, to.copyNumber).totalHits should be (100)
  }

  test("Search lots of column maps by copy number") {
    // Make sure that searchLotsOfColumnMapsByCopyNumber returns more than the standard
    // 10-result page of data. Start by creating column maps for a very wide dataset.
    val columns = (1 to 1000).map { idx => ColumnMap("wide-dataset", 1, idx, "col" + idx) }
    columns.foreach(client.putColumnMap(_, refresh = false))
    client.refresh()

    val retrieved = client.searchLotsOfColumnMapsByCopyNumber("wide-dataset", 1)
    retrieved.thisPage.sortBy(_.systemColumnId) should be (columns)
  }

  test("Put, get and delete column map") {
    client.columnMap(datasets(0), 1, "col1-1111") should not be 'defined
    client.columnMap(datasets(0), 1, "col2-2222") should not be 'defined

    val colMap = ColumnMap(datasets(0), 1, 1, "col1-1111")
    client.putColumnMap(colMap, refresh = true)

    val colMap1 = client.columnMap(datasets(0), 1, "col1-1111")
    colMap1 should be (Some(colMap))
    val colMap2 = client.columnMap(datasets(0), 1, "col2-2222")
    colMap2 should not be 'defined

    client.deleteColumnMap(colMap.datasetId, colMap.copyNumber, colMap.userColumnId)

    client.columnMap(datasets(0), 1, "col1-1111") should not be 'defined
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

    client.datasetCopy(datasets(0), 3) should be (Some(copies(datasets(0))(2)))
    client.datasetCopy(datasets(0), 4) should not be 'defined

    client.putDatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished, refresh = true)
    client.datasetCopy(datasets(0), 4) should be
      (Some(DatasetCopy(datasets(0), 4, 20, LifecycleStage.Unpublished)))

    client.deleteDatasetCopy(datasets(0), 4)
    client.datasetCopy(datasets(0), 4) should not be 'defined
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)

    client.deleteDatasetCopiesByDataset(datasets(0))
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
  }

  test("Get latest copy of dataset") {
    client.datasetCopyLatest(datasets(0)) should be ('defined)
    client.datasetCopyLatest(datasets(0)).get.copyNumber should be (3)

    client.datasetCopyLatest("foo") should not be 'defined
  }

  test("Get latest copy of dataset by stage") {
    client.datasetCopyLatest(datasets(0), Some(copy.Unpublished)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(copy.Unpublished)).get.copyNumber should be (3)
    client.datasetCopyLatest(datasets(0), Some(copy.Published)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(copy.Published)).get.copyNumber should be (2)
    client.datasetCopyLatest(datasets(0), Some(copy.Snapshotted)) should be ('defined)
    client.datasetCopyLatest(datasets(0), Some(copy.Snapshotted)).get.copyNumber should be (1)
  }

  test("Update dataset copy version") {
    client.putDatasetCopy(datasets(1), 1, 2, LifecycleStage.Unpublished, refresh = true)

    val current = client.datasetCopy(datasets(1), 1)
    current should be ('defined)
    current.get.version should be (2)
    current.get.stage should be (LifecycleStage.Unpublished)

    client.updateDatasetCopyVersion(
      current.get.copy(version = 5, stage = LifecycleStage.Published),
      refresh = true)

    client.datasetCopy(datasets(1), 1) should be ('defined)
    client.datasetCopy(datasets(1), 1).get.version should be (5)
    client.datasetCopy(datasets(1), 1).get.stage should be (LifecycleStage.Published)
  }

  test("Delete dataset copy by copy number") {
    client.putDatasetCopy(datasets(0), 1, 50L, LifecycleStage.Unpublished, refresh = true)
    client.putDatasetCopy(datasets(0), 2, 100L, LifecycleStage.Published, refresh = true)

    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should be ('defined)

    client.deleteDatasetCopy(datasets(0), 2)

    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should not be ('defined)
  }

  test("suggest is case insensitive and supports numbers and symbols") {
    val column = ColumnMap(datasets(0), 1, 1, "col1-1111")
    val fool = FieldValue(datasets(0), 1, 1, 42L, "fool")
    val food = FieldValue(datasets(0), 1, 1, 43L, "FOOD")
    val date = FieldValue(datasets(0), 1, 1, 44L, "04/2014")
    val sym  = FieldValue(datasets(0), 1, 1, 45L, "@giraffe")

    client.indexFieldValue(fool, refresh = true)
    client.indexFieldValue(food, refresh = true)
    client.indexFieldValue(date, refresh = true)
    client.indexFieldValue(sym, refresh = true)

    val suggestions = client.suggest(column, 10, "foo", Fuzziness.AUTO, 3, 1)

    // Urmila is scratching her head about what size() represents,
    // if there are 2 items returned but size() == 1
    suggestions.size() should be(1)
    suggestions.toString should include("\"options\" : [")
    suggestions.toString should include(food.value)
    suggestions.toString should include(fool.value)

    val suggestionsUpper = client.suggest(column, 10, "foo", Fuzziness.AUTO, 3, 1)

    suggestionsUpper.size() should be(1)
    suggestionsUpper.toString should include("\"options\" : [")
    suggestionsUpper.toString should include(food.value)
    suggestionsUpper.toString should include(fool.value)

    val suggestionsNum = client.suggest(column, 10, "0", Fuzziness.AUTO, 3, 1)

    suggestionsNum.size() should be(1)
    suggestionsNum.toString should include("\"options\" : [")
    suggestionsNum.toString should include(date.value)

    val suggestionsSym = client.suggest(column, 10, "@", Fuzziness.AUTO, 3, 1)

    suggestionsSym.size() should be(1)
    suggestionsSym.toString should include("\"options\" : [")
    suggestionsSym.toString should include(sym.value)
  }

  ignore("samples") {
    val column = ColumnMap(datasets(0), 1, 1, "col1-1111")

    val samples = client.sample(column, 10)

    samples.totalHits should be(5)
    samples.aggs should contain(BucketCount(makeRowData(1, 1), 1))
    samples.aggs should contain(BucketCount(makeRowData(1, 2), 1))
    samples.aggs should contain(BucketCount(makeRowData(1, 3), 1))
    samples.aggs should contain(BucketCount(makeRowData(1, 4), 1))
    samples.aggs should contain(BucketCount(makeRowData(1, 5), 1))
  }

  ignore("lots of samples") {
    val ds = datasets(0)
    val cp = copies(ds)(1)
    val col = ColumnMap(ds, cp.copyNumber, 42L, "col42")
    val lots = 1000

    val docs = for {row <- 1 to lots} yield {
      FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, row, makeRowData(col.systemColumnId, row))
    }
    val expected = docs
      .groupBy(q => q.value).map(r => BucketCount(r._1, r._2.length))
      .toSeq.sortBy(_.key)
    docs.foreach(client.indexFieldValue(_, refresh = false))
    client.refresh()

    val retrieved = client.sample(col, lots)
    retrieved.aggs.sortBy(_.key) should be(expected)
  }

  ignore("sort by frequency") {
    val ds = datasets(0)
    val cp = copies(ds)(1)
    val col = ColumnMap(ds, cp.copyNumber, 47L, "col47")
    val generated = 32
    val selected = 10

    val docs = for {row <- 1 to generated} yield {
      FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, row, Math.log(row).floor.toString)
    }
    val expected = docs
      .groupBy(q => q.value).map(r => BucketCount(r._1, r._2.length))
      .toSeq.sortBy(-_.docCount)
      .take(selected)
    docs.foreach(client.indexFieldValue(_, refresh = false))
    client.refresh()

    val retrieved = client.sample(col, selected)
    retrieved.aggs should be(expected)
  }
}
