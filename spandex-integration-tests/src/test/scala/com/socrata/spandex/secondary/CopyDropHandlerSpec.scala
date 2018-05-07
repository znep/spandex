package com.socrata.spandex.secondary

import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.SpandexIntegrationTest
import com.socrata.spandex.common.client._

class CopyDropHandlerSpec extends FunSuiteLike
  with Matchers
  with PropertyChecks
  with SpandexIntegrationTest {

  override def copies(dataset: String): Seq[DatasetCopy] = {
    val snapshot = DatasetCopy(dataset, 1, 5, LifecycleStage.Snapshotted) // scalastyle:ignore magic.number
    val published = DatasetCopy(dataset, 2, 10, LifecycleStage.Published) // scalastyle:ignore magic.number
    val workingCopy = DatasetCopy(dataset, 3, 15, LifecycleStage.Unpublished) // scalastyle:ignore magic.number
    val published2 = DatasetCopy(dataset, 4, 20, LifecycleStage.Published) // scalastyle:ignore magic.number
    Seq(snapshot, published, workingCopy, published2).sortBy(_.copyNumber)
  }

  // Make batches teensy weensy to expose any batching issues
  val handler = new VersionEventsHandler(client, 2)

  test("drop unpublished copies") {
    val datasetName = datasets(0)
    val copiesBefore = List(Snapshotted, Unpublished, Discarded).flatMap { stage =>
      client.datasetCopiesByStage(datasetName, stage)
    }
    copiesBefore should not be empty

    // set last published copy to snapshotted, simulating behavior of PublishHandler
    val lastPublished = client.datasetCopy(datasetName, 2).map(_.copy(stage=LifecycleStage.Snapshotted)).get
    client.updateDatasetCopyVersion(lastPublished, refresh = true)
    client.refresh()

    new CopyDropHandler(client).dropUnpublishedCopies(datasetName)
    client.refresh()

    val copiesAfter = List(Snapshotted, Unpublished, Discarded).flatMap { stage =>
      client.datasetCopiesByStage(datasetName, stage)
    }

    copiesAfter shouldBe empty

    val publishedCopy = client.datasetCopyLatest(datasetName, Some(Published))
    publishedCopy should be(Some(DatasetCopy(datasetName, 4, 20, LifecycleStage.Published)))
  }
}

