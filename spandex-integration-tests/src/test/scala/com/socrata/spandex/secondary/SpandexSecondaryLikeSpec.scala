package com.socrata.spandex.secondary

import java.math.BigDecimal

import com.rojoma.simplearm.util.unmanaged
import com.socrata.datacoordinator.id.{ColumnId, CopyId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.environment.ColumnName
import com.socrata.soql.types._
import com.socrata.spandex.common._
import com.socrata.spandex.common.client.{ColumnMap, ColumnValue, DatasetCopy}
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import org.joda.time.DateTime
import org.scalatest.{FunSuiteLike, Matchers}

class TestSpandexSecondary(
    override val client: SpandexElasticSearchClient,
    override val index: String,
    override val resyncBatchSize: Int,
    override val maxValueLength: Int)
  extends SpandexSecondaryLike {

  def shutdown(): Unit = client.close()
}

// scalastyle:off
class SpandexSecondaryLikeSpec extends FunSuiteLike
  with Matchers
  with SpandexIntegrationTest {

  val secondary = new TestSpandexSecondary(client.client, indexName, 5000, 64)

  test("drop dataset") {
    client.searchColumnValuesByDataset(datasets(0)).totalHits should be (45)
    client.searchColumnValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (3)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (9)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)

    secondary.dropDataset(datasets(0), None)

    client.searchColumnValuesByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnValuesByDataset(datasets(1)).totalHits should be (45)
    client.searchCopiesByDataset(datasets(0)).totalHits should be (0)
    client.searchCopiesByDataset(datasets(1)).totalHits should be (3)
    client.searchColumnMapsByDataset(datasets(0)).totalHits should be (0)
    client.searchColumnMapsByDataset(datasets(1)).totalHits should be (9)
  }

  test("drop copy") {
    client.searchColumnValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(0), 2).totalHits should be (15)
    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should be ('defined)
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (3)

    val datasetInfo = DatasetInfo(datasets(0), "en-US", Array.empty, Some(datasets(0)))
    val copyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Published, 10, DateTime.now)
    secondary.dropCopy(datasetInfo, copyInfo, None, isLatestCopy = true)

    client.searchColumnValuesByCopyNumber(datasets(0), 1).totalHits should be (15)
    client.searchColumnValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
    client.datasetCopy(datasets(0), 1) should be ('defined)
    client.datasetCopy(datasets(0), 2) should not be 'defined
    client.searchColumnMapsByCopyNumber(datasets(0), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(0), 2).totalHits should be (0)
  }

  test("drop future copy") {
    client.datasetCopy(datasets(0), 7) should not be 'defined
    val datasetInfo = DatasetInfo(datasets(0), "en-US", Array.empty, Some(datasets(0)))
    val copyInfo = CopyInfo(new CopyId(200), 7, LifecycleStage.Published, 77, DateTime.now)
    secondary.dropCopy(datasetInfo, copyInfo, None, isLatestCopy = true)
  }

  test("resync") {
    // Add some stale data related to this dataset, which should be cleaned up in the resync
    client.indexColumnValue(ColumnValue("zoo-animals", 5, 3, "marmot", 1))
    client.putColumnMap(ColumnMap("zoo-animals", 5, 3, "species"), refresh = true)
    client.putDatasetCopy("zoo-animals", 5, 5, LifecycleStage.Unpublished, refresh = true)

    client.searchCopiesByDataset("zoo-animals").totalHits should be (1)
    client.searchColumnMapsByDataset("zoo-animals").totalHits should be (1)
    client.searchColumnValuesByDataset("zoo-animals").totalHits should be (1)

    val datasetInfo = DatasetInfo("zoo-animals", "en-US", Array.empty, Some("zoo-animals"))
    val copyInfo = CopyInfo(new CopyId(100), 5, LifecycleStage.Published, 15, DateTime.now)
    val schema = ColumnIdMap[ColumnInfo[SoQLType]](
      new ColumnId(1) -> ColumnInfo[SoQLType](new ColumnId(1), new UserColumnId(":id"), Some(ColumnName(":id")), SoQLID, true, false, false, None),
      new ColumnId(2) -> ColumnInfo[SoQLType](new ColumnId(2), new UserColumnId("animal"), Some(ColumnName("animal")), SoQLText, false, false, false, None),
      new ColumnId(3) -> ColumnInfo[SoQLType](new ColumnId(3), new UserColumnId("class"), Some(ColumnName("class")), SoQLText, false, false, false, None),
      new ColumnId(4) -> ColumnInfo[SoQLType](new ColumnId(4), new UserColumnId("population"), Some(ColumnName("population")), SoQLNumber, false, false, false, None)
    )
    val rows = Seq(
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(10),
        new ColumnId(2) -> new SoQLText("giraffe"),
        new ColumnId(3) -> new SoQLText("mammalia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(6))),
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(12),
        new ColumnId(2) -> new SoQLText("axolotl"),
        new ColumnId(3) -> new SoQLText("amphibia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(2))),
      ColumnIdMap[SoQLValue](
        new ColumnId(1) -> new SoQLID(14),
        new ColumnId(2) -> new SoQLText("ostrich"),
        new ColumnId(3) -> new SoQLText("avia"),
        new ColumnId(4) -> new SoQLNumber(new BigDecimal(3)))
    )

    secondary.resync(datasetInfo, copyInfo, schema, None, unmanaged(rows.iterator), Seq.empty, isLatestLivingCopy = true)

    val copies = client.searchCopiesByDataset("zoo-animals")
    copies.totalHits should be (1)
    copies.thisPage.map(_.result).head should be (
      DatasetCopy("zoo-animals", copyInfo.copyNumber, copyInfo.dataVersion, copyInfo.lifecycleStage))

    client.searchColumnMapsByDataset("zoo-animals").totalHits should be (2)
    val columnMaps = client.searchColumnMapsByDataset("zoo-animals").thisPage.map(_.result).sortBy(_.systemColumnId)
    columnMaps(0) should be (ColumnMap("zoo-animals", copyInfo.copyNumber, 2, "animal"))
    columnMaps(1) should be (ColumnMap("zoo-animals", copyInfo.copyNumber, 3, "class"))

    client.searchColumnValuesByDataset("zoo-animals").totalHits should be (6)
    val columnValues = client.searchColumnValuesByDataset("zoo-animals").thisPage.map(_.result).sortBy(cv => (cv.columnId, cv.value))
    columnValues should contain theSameElementsInOrderAs(
      List(
        ColumnValue("zoo-animals", copyInfo.copyNumber, 2, "axolotl", 1),
        ColumnValue("zoo-animals", copyInfo.copyNumber, 2, "giraffe", 1),
        ColumnValue("zoo-animals", copyInfo.copyNumber, 2, "ostrich", 1),
        ColumnValue("zoo-animals", copyInfo.copyNumber, 3, "amphibia", 1),
        ColumnValue("zoo-animals", copyInfo.copyNumber, 3, "avia", 1),
        ColumnValue("zoo-animals", copyInfo.copyNumber, 3, "mammalia", 1)))
  }
}
