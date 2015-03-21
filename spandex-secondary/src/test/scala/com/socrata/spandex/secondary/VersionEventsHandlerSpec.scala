package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.CopyId
import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.{TestESClient, DatasetCopy}
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.scalatest.prop.PropertyChecks

class VersionEventsHandlerSpec extends FunSuiteLike
                                  with Matchers
                                  with BeforeAndAfterAll
                                  with BeforeAndAfterEach
                                  with PropertyChecks {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)

  override def afterAll(): Unit = client.close()

  test("Throw an exception if data version is invalid") {
    val datasetInfo = DatasetInfo("alpha.75", "en-US", Array.empty[Byte])
    val invalidDataVersion = 0
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator
    val handler = new VersionEventsHandler(client)

    an [IllegalArgumentException] should be thrownBy handler.handle(datasetInfo.internalName, invalidDataVersion, events)
  }

  test("Throw an exception if multiple working copies are encountered in the same event sequence") {
    val datasetInfo = DatasetInfo("alpha.75", "en-US", Array.empty[Byte])
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo), WorkingCopyCreated(copyInfo)).iterator
    val handler = new VersionEventsHandler(client)

    a [UnsupportedOperationException] should be thrownBy handler.handle(datasetInfo.internalName, 1, events)
  }

  test("Throw an exception if the working copy to be created already exists") {
    val dataVersion = 12
    val datasetInfo = DatasetInfo("alpha.75", "en-US", Array.empty[Byte])
    val copyInfo = CopyInfo(new CopyId(100), 1, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator
    val handler = new VersionEventsHandler(client)

    client.putDatasetCopy(datasetInfo.internalName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage)
    Thread.sleep(1000) // Wait for ES to index document

    a [ResyncSecondaryException] should be thrownBy handler.handle(datasetInfo.internalName, 1, events)
  }

  test("A dataset copy document is added to the index when a new working copy is created") {
    val dataVersion = 15
    val datasetInfo = DatasetInfo("alpha.76", "en-US", Array.empty[Byte])
    val copyInfo = CopyInfo(new CopyId(100), 45, LifecycleStage.Unpublished, 1, DateTime.now)
    val events = Seq(WorkingCopyCreated(copyInfo)).iterator
    val handler = new VersionEventsHandler(client)

    client.deleteAllDatasetCopies()
    client.getDatasetCopy(datasetInfo.internalName, copyInfo.copyNumber) should not be 'defined

    handler.handle(datasetInfo.internalName, dataVersion, events)
    val maybeCopyRecord = client.getDatasetCopy(datasetInfo.internalName, copyInfo.copyNumber)
    maybeCopyRecord should be ('defined)
    maybeCopyRecord.get should be (DatasetCopy(datasetInfo.internalName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage))
  }

  test("Latest copy of dataset is dropped on Truncate event")(pending)
}
