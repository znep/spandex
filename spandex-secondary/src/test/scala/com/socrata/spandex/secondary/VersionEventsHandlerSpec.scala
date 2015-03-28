package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId, RowId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLValue, SoQLNumber, SoQLText}
import com.socrata.spandex.common.{TestESData, SpandexConfig}
import com.socrata.spandex.common.client.{ColumnMap, TestESClient, DatasetCopy}
import java.math.BigDecimal
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.scalatest.prop.PropertyChecks

// scalastyle:off
class VersionEventsHandlerSpec extends FunSuiteLike
                                  with Matchers
                                  with BeforeAndAfterAll
                                  with BeforeAndAfterEach
                                  with PropertyChecks
                                  with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)
  val handler = new VersionEventsHandler(client)

  override def beforeEach(): Unit = {
    client.deleteAllDatasetCopies()
    bootstrapData()
  }
  override def afterEach(): Unit = removeBootstrapData()
  override def afterAll(): Unit = client.close()

  test("All - throw an exception if data version is invalid") {
    val invalidDataVersion = 0
    val events = Seq(Truncated).iterator

    an [IllegalArgumentException] should be thrownBy handler.handle("alpha.75", invalidDataVersion, events)
  }

  test("WorkingCopyCreated - throw an exception if multiple working copies are encountered in the same event sequence") {
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo), WorkingCopyCreated(copyInfo)).iterator

    a [UnsupportedOperationException] should be thrownBy handler.handle("alpha.75", 1, events)
  }

  test("WorkingCopyCreated - throw an exception if the copy to be created already exists") {
    val dataVersion = 12
    val dataset = "alpha.75"
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator

    client.putDatasetCopy(dataset, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage)

    a [ResyncSecondaryException] should be thrownBy handler.handle(dataset, 1, events)
  }

  test("WorkingCopyCreated - a new dataset copy should be added to the index") {
    val dataVersion = 15
    val dataset = "alpha.76"
    val copyInfo = CopyInfo(new CopyId(100), 45, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator

    client.getDatasetCopy(dataset, copyInfo.copyNumber) should not be 'defined

    handler.handle(dataset, dataVersion, events)
    val maybeCopyRecord = client.getDatasetCopy(dataset, copyInfo.copyNumber)
    maybeCopyRecord should be ('defined)
    maybeCopyRecord.get should be (DatasetCopy(dataset, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage))
  }

  test("Truncate - field values in latest copy of dataset should be dropped") {
    val expectedLatestBefore = copies(datasets(0)).last

    val latestBefore = client.getLatestCopyForDataset(datasets(0))
    latestBefore should be ('defined)
    latestBefore.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestBefore.get.version should be (expectedLatestBefore.version)
    client.searchFieldValuesByCopyNumber(datasets(0), expectedLatestBefore.copyNumber).totalHits should be (15)

    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(Truncated).iterator)

    val latestAfter = client.getLatestCopyForDataset(datasets(0))
    latestAfter should be ('defined)
    latestAfter.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestAfter.get.version should be (expectedLatestBefore.version + 1)
    client.searchFieldValuesByCopyNumber(datasets(0), expectedLatestBefore.copyNumber).totalHits should be (0)
  }

  test("ColumnCreated - don't create column map for non-SoQLText columns") {
    val numCol = ColumnInfo(new ColumnId(10), new UserColumnId("nums-1234"), SoQLNumber, false, false, false)
    val textCol = ColumnInfo(new ColumnId(20), new UserColumnId("text-1234"), SoQLText, false, false, false)

    // Create column
    handler.handle(datasets(0), 3, Seq(ColumnCreated(numCol), ColumnCreated(textCol)).iterator)

    val latest = client.getLatestCopyForDataset(datasets(0))
    client.getColumnMap(datasets(0), latest.get.copyNumber, numCol.id.underlying) should not be 'defined
    val textColMap = client.getColumnMap(datasets(0), latest.get.copyNumber, textCol.id.underlying)
    textColMap should be
      (Some(ColumnMap(datasets(0), latest.get.copyNumber, textCol.systemId.underlying, textCol.id.underlying)))
  }

  test("ColumnCreated and ColumnRemoved") {
    val expectedLatestBefore = copies(datasets(0)).last
    val info = ColumnInfo(new ColumnId(3), new UserColumnId("blah-1234"), SoQLText, false, false, false)

    val latestBeforeAdd = client.getLatestCopyForDataset(datasets(0))
    latestBeforeAdd should be ('defined)
    latestBeforeAdd.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestBeforeAdd.get.version should be (expectedLatestBefore.version)
    client.getColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should not be 'defined

    // Create column
    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(ColumnCreated(info)).iterator)

    client.getColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should be
      (Some(ColumnMap(datasets(0), 2, info.systemId.underlying, info.id.underlying)))

    // Pretend we added some data in between (which actually got added during bootstrap)
    client.searchFieldValuesByColumnId(datasets(0), 2, 3).totalHits should be (5)

    // Remove column
    handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(ColumnRemoved(info)).iterator)

    val latestAfterDelete = client.getLatestCopyForDataset(datasets(0))
    latestAfterDelete should be ('defined)
    latestAfterDelete.get.copyNumber should be (expectedLatestBefore.copyNumber)
    latestAfterDelete.get.version should be (expectedLatestBefore.version + 1)
    client.searchFieldValuesByColumnId(
      datasets(0), expectedLatestBefore.copyNumber, info.systemId.underlying).totalHits should be (0)
    client.getColumnMap(datasets(0), expectedLatestBefore.copyNumber, info.id.underlying) should not be 'defined
  }

  test("WorkingCopyPublished - latest copy of dataset should be set to Published") {
   val expectedLatestBefore = copies(datasets(0))(2)
   client.getLatestCopyForDataset(datasets(0)) should be (Some(expectedLatestBefore))

   handler.handle(datasets(0), expectedLatestBefore.version + 1, Seq(WorkingCopyPublished).iterator)

   val expectedAfter = expectedLatestBefore.updateCopy(expectedLatestBefore.version + 1, LifecycleStage.Published)
   client.getLatestCopyForDataset(datasets(0)) should be (Some(expectedAfter))
  }

  test("WorkingCopyDropped - throw an exception if the copy is the initial copy") {
    client.putDatasetCopy("wcd-test-initial-copy", 1, 1, LifecycleStage.Unpublished)

    val expectedBefore = Some(DatasetCopy("wcd-test-initial-copy", 1, 1, LifecycleStage.Unpublished))
    client.getLatestCopyForDataset("wcd-test-initial-copy") should be(expectedBefore)

    an [UnsupportedOperationException] should be thrownBy
      handler.handle("wcd-test-initial-copy", 2, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - throw an exception if the copy is in the wrong stage") {
    client.putDatasetCopy("wcd-test-published", 2, 2, LifecycleStage.Published)

    val expectedBefore = Some(DatasetCopy("wcd-test-published", 2, 2, LifecycleStage.Published))
    client.getLatestCopyForDataset("wcd-test-published") should be(expectedBefore)

    an [UnsupportedOperationException] should be thrownBy
      handler.handle("wcd-test-published", 3, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - latest working copy should be dropped") {
    val expectedBefore = copies(datasets(1)).last
    client.getLatestCopyForDataset(datasets(1)) should be(Some(expectedBefore))
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)

    handler.handle(datasets(1), expectedBefore.version + 1, Seq(WorkingCopyDropped).iterator)

    val expectedAfter = copies(datasets(1))(1).updateCopy(expectedBefore.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfter))
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (0)
  }

  test("SnapshotDropped - throw an exception if the copy is in the wrong stage") {
    client.putDatasetCopy("sd-test-notsnapshot", 2, 2, LifecycleStage.Unpublished)

    val expectedBefore = Some(DatasetCopy("sd-test-notsnapshot", 2, 2, LifecycleStage.Unpublished))
    client.getLatestCopyForDataset("sd-test-notsnapshot") should be (expectedBefore)

    val copyInfo = CopyInfo(new CopyId(100), 2, LifecycleStage.Unpublished, 2, DateTime.now)
    val events =  Seq(SnapshotDropped(copyInfo)).iterator
    an [UnsupportedOperationException] should be thrownBy handler.handle("sd-test-notsnapshot", 3, events)
  }

  test("SnapshotDropped - the specified snapshot should be dropped") {
    val expectedBefore = copies(datasets(1)).last
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedBefore))
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)

    val snapshot = copies(datasets(1)).head
    val copyInfo = CopyInfo(new CopyId(100), snapshot.copyNumber, snapshot.stage, snapshot.version, DateTime.now)
    handler.handle(datasets(1), expectedBefore.version + 1, Seq(SnapshotDropped(copyInfo)).iterator)

    val expectedAfter = expectedBefore.updateCopy(expectedBefore.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfter))
    client.searchFieldValuesByCopyNumber(datasets(1), 1).totalHits should be (0)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
    client.searchColumnMapsByCopyNumber(datasets(1), 1).totalHits should be (0)
    client.searchColumnMapsByCopyNumber(datasets(1), 2).totalHits should be (3)
    client.searchColumnMapsByCopyNumber(datasets(1), 3).totalHits should be (3)
  }

  test("RowDataUpdated - Insert and update") {
    val expectedBeforeInsert = copies(datasets(1)).last
    val insert = Insert(new RowId(6), ColumnIdMap[SoQLValue](
      new ColumnId(5) -> SoQLText("index me!"), new ColumnId(9) -> SoQLNumber(new BigDecimal(5))))

    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedBeforeInsert))
    client.searchFieldValuesByCopyNumber(
      datasets(1), expectedBeforeInsert.copyNumber).totalHits should be (15)
    client.searchFieldValuesByRowId(
      datasets(1), expectedBeforeInsert.copyNumber, insert.systemId.underlying).totalHits should be (0)


    val insertEvents = Seq(RowDataUpdated(Seq[Operation](insert))).iterator
    handler.handle(datasets(1), expectedBeforeInsert.version + 1, insertEvents)

    val expectedAfterInsert = expectedBeforeInsert.updateCopy(expectedBeforeInsert.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfterInsert))
    client.searchFieldValuesByCopyNumber(
      datasets(1), expectedBeforeInsert.copyNumber).totalHits should be (16)
    val newEntries = client.searchFieldValuesByRowId(
      datasets(1), expectedBeforeInsert.copyNumber, insert.systemId.underlying)
    newEntries.totalHits should be (1)
    newEntries.thisPage(0).columnId should be (5)
    newEntries.thisPage(0).value should be ("index me!")

    val update = Update(new RowId(2), ColumnIdMap[SoQLValue](
      new ColumnId(2) -> SoQLText("updated data2"), new ColumnId(3) -> SoQLText("updated data3")))(None)

    val updateEvents = Seq(RowDataUpdated(Seq[Operation](update))).iterator
    handler.handle(datasets(1), expectedAfterInsert.version + 1, updateEvents)

    val expectedAfter = expectedAfterInsert.updateCopy(expectedAfterInsert.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfter))
    client.searchFieldValuesByCopyNumber(
      datasets(1), expectedAfterInsert.copyNumber).totalHits should be (16)
    val updatedRow = client.searchFieldValuesByRowId(
      datasets(1), expectedAfterInsert.copyNumber, update.systemId.underlying)
    updatedRow.totalHits should be (3)
    val updatedFieldValues = updatedRow.thisPage.sortBy(_.columnId).toSeq
    updatedFieldValues(0).columnId should be (1)
    updatedFieldValues(0).value should be ("data column 1 row 2")
    updatedFieldValues(1).columnId should be (2)
    updatedFieldValues(1).value should be ("updated data2")
    updatedFieldValues(2).columnId should be (3)
    updatedFieldValues(2).value should be ("updated data3")
  }

  test("RowDataUpdated - Delete") {
    val expectedBefore = copies(datasets(1)).last
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedBefore))
    client.searchFieldValuesByCopyNumber(datasets(1), expectedBefore.copyNumber).totalHits should be (15)
    client.searchFieldValuesByRowId(datasets(1), expectedBefore.copyNumber, 2).totalHits should be (3)
    client.searchFieldValuesByRowId(datasets(1), expectedBefore.copyNumber, 5).totalHits should be (3)

    val events = Seq(RowDataUpdated(
      Seq[Operation](Delete(new RowId(2))(None), Delete(new RowId(5))(None)))).iterator
    handler.handle(datasets(1), expectedBefore.version + 1, events)

    val expectedAfter = expectedBefore.updateCopy(expectedBefore.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfter))
    client.searchFieldValuesByCopyNumber(datasets(1), expectedAfter.copyNumber).totalHits should be (9)
    client.searchFieldValuesByRowId(datasets(1), expectedAfter.copyNumber, 2).totalHits should be (0)
    client.searchFieldValuesByRowId(datasets(1), expectedAfter.copyNumber, 5).totalHits should be (0)
  }

  test("DataCopied - all field values from last published copy should be copied to latest copy") {
    // Remove bootstrapped data on working copy
    client.deleteFieldValuesByCopyNumber(datasets(1), 3)

    val expectedBefore = copies(datasets(1)).last
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedBefore))
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (0)

    handler.handle(datasets(1), expectedBefore.version + 1, Seq(DataCopied).iterator)

    val expectedAfter = expectedBefore.updateCopy(expectedBefore.version + 1)
    client.getLatestCopyForDataset(datasets(1)) should be (Some(expectedAfter))
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)
    client.searchFieldValuesByCopyNumber(datasets(1), 3).totalHits should be (15)
  }
}
