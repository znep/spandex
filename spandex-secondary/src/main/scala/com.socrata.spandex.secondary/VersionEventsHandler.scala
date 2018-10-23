package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.SoQLText
import com.socrata.spandex.common.Timings
import com.socrata.spandex.common.client._

class VersionEventsHandler(
    client: SpandexElasticSearchClient,
    maxValueLength: Int,
    refresh: RefreshPolicy = Eventually)
  extends SecondaryEventLogger {

  def handle(datasetName: String, // scalastyle:ignore cyclomatic.complexity method.length
             dataVersion: Long,
             events: Iterator[Event]): Unit = {
    require(dataVersion > 0, s"Unexpected value for data version: $dataVersion")

    val startTime = Timings.now

    // First, handle any working copy events
    // NOTE: do not return until refresh, since the subsequent call expects this dataset copy to be indexed
    val remainingEvents = new WorkingCopyCreatedHandler(client, refresh = BeforeReturning)
      .go(datasetName, dataVersion, events)

    // Find the latest dataset copy number. This *should* exist since
    // we have already handled creation of any initial working copies.
    val latest = client.datasetCopyLatest(datasetName).getOrElse(
      throw InvalidStateBeforeEvent(s"Couldn't get latest copy number for dataset $datasetName"))

    // Now handle everything else
    remainingEvents.foreach { event =>
      logEventReceived(event)
      event match {
        case DataCopied =>
          val latestPublished = client.datasetCopyLatest(datasetName, Some(Published)).getOrElse(
            throw InvalidStateBeforeEvent(s"Could not find a published copy to copy data from"))
          logDataCopied(datasetName, latestPublished.copyNumber, latest.copyNumber)
          client.copyColumnValues(from = latestPublished, to = latest, refresh)
        case RowDataUpdated(ops) =>
          new RowOpsHandler(client, maxValueLength, refresh).go(datasetName, latest.copyNumber, ops)
        case SnapshotDropped(info) =>
          new CopyDropHandler(client, refresh).dropSnapshot(datasetName, info)
        case WorkingCopyDropped =>
          new CopyDropHandler(client, refresh).dropWorkingCopy(datasetName, latest)
        case WorkingCopyPublished =>
          new PublishHandler(client, refresh = BeforeReturning).go(datasetName, latest)
          new CopyDropHandler(client, refresh).dropUnpublishedCopies(datasetName)
        case ColumnCreated(info) =>
          if (info.typ == SoQLText) {
            logColumnCreated(datasetName, latest.copyNumber, info)
            client.putColumnMap(ColumnMap(datasetName, latest.copyNumber, info), refresh)
          }
        case ColumnRemoved(info) =>
          logColumnRemoved(datasetName, latest.copyNumber, info.id.underlying)
          client.deleteColumnValuesByColumnId(datasetName, latest.copyNumber, info.systemId.underlying, refresh)
          client.deleteColumnMap(datasetName, latest.copyNumber, info.id.underlying, refresh)
        case Truncated =>
          logTruncate(datasetName, latest.copyNumber)
          client.deleteColumnValuesByCopyNumber(datasetName, latest.copyNumber, refresh)
        case LastModifiedChanged(lm) =>
        case RowIdentifierSet(info) =>
        case RowIdentifierCleared(info) =>
        case SystemRowIdentifierChanged(info) =>
        case VersionColumnChanged(info) =>
        case RollupCreatedOrUpdated(info) =>
        case RollupDropped(info) =>
        case FieldNameUpdated(info) =>
        case ComputationStrategyCreated(_) =>
        case ComputationStrategyRemoved(_) =>
        case RowsChangedPreview(truncated, inserted, updated, deleted) =>
        // These events don't result in changed data or publication status, so no-op.
        // The data version is updated below.
        case WorkingCopyCreated(info) =>
          // We have handled all WorkingCopyCreated events above. This should never happen.
          throw new UnsupportedOperationException("Unexpected WorkingCopyCreated event")
      }
    }

    client.deleteNonPositiveCountColumnValues(datasetName, latest.copyNumber, refresh)

    // Finally, get whatever the new latest copy is and bump its data version.
    logDataVersionBump(datasetName, latest.copyNumber, latest.version, dataVersion)
    val finalLatest = client.datasetCopyLatest(datasetName).getOrElse(
      throw InvalidStateAfterEvent(s"Couldn't get latest copy number for dataset $datasetName"))
    client.updateDatasetCopyVersion(finalLatest.copy(version = dataVersion), refresh)
    client.refresh()

    val timeElapsed = Timings.elapsedInMillis(startTime)
    logVersionEventsProcessed(datasetName, finalLatest.copyNumber, finalLatest.version, timeElapsed)
  }
}
