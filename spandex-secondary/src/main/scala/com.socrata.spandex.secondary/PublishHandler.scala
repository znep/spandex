package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.{LifecycleStage, ResyncSecondaryException}
import com.socrata.spandex.common.client.{DatasetCopy, Eventually, Published, RefreshPolicy, SpandexElasticSearchClient}

class PublishHandler(
    client: SpandexElasticSearchClient,
    refresh: RefreshPolicy = Eventually)
  extends SecondaryEventLogger {

  def go(datasetName: String, latest: DatasetCopy): Unit = {
    if (latest.stage != LifecycleStage.Unpublished) {
      throw ResyncSecondaryException(
        s"Expected latest copy to be Unpublished when receiving WorkingCopyPublished event. " +
          s"Actual: ${latest.stage}.")
    }

    logWorkingCopyPublished(datasetName, latest.copyNumber)

    val maybeLastPublished = client.datasetCopyLatest(datasetName, Some(Published))

    // Set the latest unpublished version to published
    client.updateDatasetCopyVersion(latest.copy(stage = LifecycleStage.Published), refresh)

    // Set the previous published version (if any) to Snapshotted
    maybeLastPublished.foreach { lastPublished =>
      client.updateDatasetCopyVersion(lastPublished.copy(stage = LifecycleStage.Snapshotted), refresh)
    }
  }
}
