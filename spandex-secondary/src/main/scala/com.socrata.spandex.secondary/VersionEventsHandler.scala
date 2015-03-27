package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.{RowId, ColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.soql.types.{SoQLValue, SoQLText}
import com.socrata.spandex.common.client._

class VersionEventsHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def handle(datasetName: String, // scalastyle:ignore cyclomatic.complexity method.length
             dataVersion: Long,
             events: Iterator[Event]): Unit = {
    require(dataVersion > 0, s"Unexpected value for data version: $dataVersion")

    // First, handle any working copy events
    val remainingEvents = WorkingCopyCreatedHandler(client).go(datasetName, dataVersion, events)

    // TODO : Decide how to deal with Elastic Search's indexing delay.
    // ES only refreshes the index at set intervals (default 1s, configurable)
    // instead of after every single write.
    // http://www.elastic.co/guide/en/elasticsearch/reference/current/_modifying_your_data.html
    // http://blog.sematext.com/2013/07/08/elasticsearch-refresh-interval-vs-indexing-performance/
    // Things we can do instead of terrible Thread.sleeps everywhere:
    // 1. Turn off the refresh interval and set ES to refresh the index after every write.
    //    Pros - consistent reads.
    //    Cons - indexing throughput is lessened, see blog post above. This may be acceptable.
    // 2. Batch up all the index updates and send them to ES in one go. This will make a single
    //    batch of events internally consistent but if we get 2 batches in quick succession there
    //    are no guarantees.

    // Find the latest dataset copy number. This *should* exist since
    // we have already handled creation of any initial working copies.
    val latest = client.getLatestCopyForDataset(datasetName).getOrElse(
      throw new UnsupportedOperationException(s"Couldn't get latest copy number for dataset $datasetName"))

    // Now handle everything else
    remainingEvents.foreach { event =>
      logger.debug("Received event: " + event)
      event match {
        case DataCopied =>
          val latestPublished = client.getLatestCopyForDataset(datasetName, publishedOnly = true).getOrElse(
            throw new UnsupportedOperationException(s"Could not find a published copy to copy data from"))
          client.copyFieldValues(from = latestPublished, to = latest)
        case RowDataUpdated(ops) =>
          RowOpsHandler(client).go(datasetName, latest.copyNumber, ops)
        case SnapshotDropped(info) =>
          CopyDropHandler(client).dropSnapshot(datasetName, info)
        case WorkingCopyDropped =>
          CopyDropHandler(client).dropWorkingCopy(datasetName, latest)
        case WorkingCopyPublished =>
          logWorkingCopyPublished(datasetName, latest.copyNumber)
          client.updateDatasetCopyVersion(latest.updateCopy(dataVersion, LifecycleStage.Published))
        case ColumnCreated(info) =>
          if (info.typ == SoQLText) {
            logColumnCreated(datasetName, latest.copyNumber, info)
            client.putColumnMap(ColumnMap(datasetName, latest.copyNumber, info))
          }
        case ColumnRemoved(info) =>
          logColumnRemoved(datasetName, latest.copyNumber, info.id.underlying)
          client.deleteFieldValuesByColumnId(datasetName, latest.copyNumber, info.systemId.underlying)
          client.deleteColumnMap(datasetName, latest.copyNumber, info.id.underlying)
        case Truncated =>
          logTruncate(datasetName, latest.copyNumber)
          client.deleteFieldValuesByCopyNumber(datasetName, latest.copyNumber)
        case LastModifiedChanged(lm) =>
        // TODO : Support if-modified-since one day
        case RowIdentifierSet(info) =>
        case RowIdentifierCleared(info) =>
        case SystemRowIdentifierChanged(info) =>
        case VersionColumnChanged(info) =>
        case RollupCreatedOrUpdated(info) =>
        case RollupDropped(info) =>
        // These events don't result in changed data or publication status, so no-op.
        // The data version is updated below.
        case WorkingCopyCreated(info) =>
          // We have handled all WorkingCopyCreated events above. This should never happen.
          throw new UnsupportedOperationException("Unexpected WorkingCopyCreated event")
      }
    }

    // Finally, get whatever the new latest copy is and bump its data version.
    logDataVersionBump(datasetName, latest.copyNumber, latest.version, dataVersion)
    val finalLatest = client.getLatestCopyForDataset(datasetName).getOrElse(
      throw new UnsupportedOperationException(s"Couldn't get latest copy number for dataset $datasetName"))
    client.updateDatasetCopyVersion(finalLatest.updateCopy(dataVersion))
  }
}
