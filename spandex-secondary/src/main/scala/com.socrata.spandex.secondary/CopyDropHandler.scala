package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.{CopyInfo, LifecycleStage}
import com.socrata.spandex.common.client.{SpandexElasticSearchClient, DatasetCopy}

case class CopyDropHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  private def checkStage(expected: LifecycleStage, actual: LifecycleStage): Unit =
    if (actual != expected) {
      throw new UnsupportedOperationException(s"Copy is in unexpected stage: $actual. Expected: $expected")
    }

  def dropSnapshot(datasetName: String, info: CopyInfo): Unit = {
    checkStage(LifecycleStage.Snapshotted, info.lifecycleStage)
    logSnapshotDropped(datasetName, info.copyNumber)
    client.deleteDatasetCopy(datasetName, info.copyNumber)
    client.deleteFieldValuesByCopyNumber(datasetName, info.copyNumber)
    // TODO : Delete column maps for copy
  }

  def dropWorkingCopy(datasetName: String, latest: DatasetCopy): Unit = {
    checkStage(LifecycleStage.Unpublished, latest.stage)

    if (latest.copyNumber < 2) {
      throw new UnsupportedOperationException("Cannot drop initial working copy")
    }

    logWorkingCopyDropped(datasetName, latest.copyNumber)
    client.deleteDatasetCopy(datasetName, latest.copyNumber)
    client.deleteFieldValuesByCopyNumber(datasetName, latest.copyNumber)
    // TODO : Delete column maps for copy
  }
}
