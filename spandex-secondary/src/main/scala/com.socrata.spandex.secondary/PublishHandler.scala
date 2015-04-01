package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client.{DatasetCopy, SpandexElasticSearchClient}

case class PublishHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def go(datasetName: String, latest: DatasetCopy): Unit = {
    if (latest.stage != LifecycleStage.Unpublished) {
      throw new UnsupportedOperationException(
        s"Expected latest copy to be Unpublished when receiving WorkingCopyPublished event. " +
          s"Actual: ${latest.stage}.")
    }

    logWorkingCopyPublished(datasetName, latest.copyNumber)

    val maybeLastPublished = client.getLatestCopyForDataset(datasetName, publishedOnly = true)

    // Set the latest unpublished version to published
    client.updateDatasetCopyVersion(
      latest.updateCopy(LifecycleStage.Published), refresh = false)

    // Set the previous published version (if any) to Snapshotted
    maybeLastPublished.foreach { lastPublished =>
      client.updateDatasetCopyVersion(
        lastPublished.updateCopy(LifecycleStage.Snapshotted), refresh = true)
    }
  }
}
