package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.{UserColumnId, ColumnId, CopyId}
import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.SoQLText
import com.socrata.spandex.common.{TestESData, SpandexConfig}
import com.socrata.spandex.common.client.{TestESClient, DatasetCopy}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.scalatest.prop.PropertyChecks

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
    Thread.sleep(1000) // Wait for ES to index document

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
    client.putDatasetCopy(datasets(0), 1, 1, LifecycleStage.Unpublished)
    client.putDatasetCopy(datasets(0), 2, 2, LifecycleStage.Published)
    Thread.sleep(1000) // Wait for ES to index document

    val latestBefore = client.getLatestCopyForDataset(datasets(0))
    latestBefore should be ('defined)
    latestBefore.get.copyNumber should be (2)
    latestBefore.get.version should be (2)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (15)

    handler.handle(datasets(0), 3, Seq(Truncated).iterator)
    Thread.sleep(1000) // Wait for ES to index document

    val latestAfter = client.getLatestCopyForDataset(datasets(0))
    latestAfter should be ('defined)
    latestAfter.get.copyNumber should be (2)
    latestAfter.get.version should be (3)
    client.searchFieldValuesByCopyNumber(datasets(0), 2).totalHits should be (0)
  }

  test("ColumnRemoved - field values for column in latest copy of dataset should be dropped") {
    client.putDatasetCopy(datasets(0), 1, 1, LifecycleStage.Unpublished)
    client.putDatasetCopy(datasets(0), 2, 2, LifecycleStage.Published)
    Thread.sleep(1000) // Wait for ES to index document

    val latestBefore = client.getLatestCopyForDataset(datasets(0))
    latestBefore should be ('defined)
    latestBefore.get.copyNumber should be (2)
    latestBefore.get.copyNumber should be (2)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(2)).totalHits should be (5)

    val toRemove = ColumnInfo(new ColumnId(1234), new UserColumnId(columns(2)), SoQLText, false, false, false)
    handler.handle(datasets(0), 3, Seq(ColumnRemoved(toRemove)).iterator)
    Thread.sleep(1000) // Wait for ES to index document

    val latestAfter = client.getLatestCopyForDataset(datasets(0))
    latestAfter should be ('defined)
    latestAfter.get.copyNumber should be (2)
    latestAfter.get.version should be (3)
    client.searchFieldValuesByColumnId(datasets(0), 2, columns(2)).totalHits should be (0)
  }

  test("WorkingCopyPublished - latest copy of dataset should be set to Published") {
    client.putDatasetCopy(datasets(0), 3, 3, LifecycleStage.Unpublished)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedBefore = Some(DatasetCopy(datasets(0), 3, 3, LifecycleStage.Unpublished))
    client.getLatestCopyForDataset(datasets(0)) should be (expectedBefore)

    handler.handle(datasets(0), 4, Seq(WorkingCopyPublished).iterator)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedAfter = Some(DatasetCopy(datasets(0), 3, 4, LifecycleStage.Published))
    client.getLatestCopyForDataset(datasets(0)) should be (expectedAfter)
  }

  test("WorkingCopyDropped - throw an exception if the copy is the initial copy") {
    client.putDatasetCopy(datasets(1), 1, 1, LifecycleStage.Unpublished)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedBefore = Some(DatasetCopy(datasets(1), 1, 1, LifecycleStage.Unpublished))
    client.getLatestCopyForDataset(datasets(1)) should be(expectedBefore)

    an [UnsupportedOperationException] should be thrownBy
      handler.handle(datasets(1), 2, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - throw an exception if the copy is in the wrong stage") {
    client.putDatasetCopy(datasets(1), 2, 2, LifecycleStage.Published)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedBefore = Some(DatasetCopy(datasets(1), 2, 2, LifecycleStage.Published))
    client.getLatestCopyForDataset(datasets(1)) should be(expectedBefore)

    an [UnsupportedOperationException] should be thrownBy
      handler.handle(datasets(1), 3, Seq(WorkingCopyDropped).iterator)
  }

  test("WorkingCopyDropped - latest working copy should be dropped") {
    client.putDatasetCopy(datasets(1), 1, 2, LifecycleStage.Published)
    client.putDatasetCopy(datasets(1), 2, 3, LifecycleStage.Unpublished)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedBefore = Some(DatasetCopy(datasets(1), 2, 3, LifecycleStage.Unpublished))
    client.getLatestCopyForDataset(datasets(1)) should be(expectedBefore)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (15)

    handler.handle(datasets(1), 4, Seq(WorkingCopyDropped).iterator)
    Thread.sleep(1000) // Wait for ES to index document

    val expectedAfter = Some(DatasetCopy(datasets(1), 1, 4, LifecycleStage.Published))
    client.getLatestCopyForDataset(datasets(1)) should be(expectedAfter)
    client.searchFieldValuesByCopyNumber(datasets(1), 2).totalHits should be (0)
  }
}
