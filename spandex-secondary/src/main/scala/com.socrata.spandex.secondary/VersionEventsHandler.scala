package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common.client.{DatasetCopy, SpandexElasticSearchClient}

class VersionEventsHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def handle(datasetName: String, // scalastyle:ignore cyclomatic.complexity method.length
             dataVersion: Long,
             events: Events): Unit = {
    require(dataVersion > 0, s"Unexpected value for data version: $dataVersion")

    // First, handle any working copy events
    val remainingEvents = handleWorkingCopyCreate(datasetName, dataVersion, events)

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
    Thread.sleep(1000) // scalastyle:ignore magic.number

    // Find the latest dataset copy number. This *should* exist since
    // we have already handled creation of any initial working copies.
    val latest = client.getLatestCopyForDataset(datasetName).getOrElse(
      throw new UnsupportedOperationException(s"Couldn't get latest copy number for dataset $datasetName"))

    // Now handle everything else
    remainingEvents.foreach {
      // TODO : DataCopied, RowDataUpdated(_), SnapshotDropped(_)
      case WorkingCopyDropped =>
        checkDroppable(latest)
        logWorkingCopyDropped(datasetName, latest.copyNumber)
        client.deleteDatasetCopy(datasetName, latest.copyNumber)
        client.deleteFieldValuesByCopyNumber(datasetName, latest.copyNumber)
      case WorkingCopyPublished =>
        logWorkingCopyPublished(datasetName, latest.copyNumber)
        client.updateDatasetCopyVersion(latest.updateCopy(dataVersion, LifecycleStage.Published))
      case ColumnRemoved(info) =>
        logColumnRemoved(datasetName, latest.copyNumber, info.id.underlying)
        client.deleteFieldValuesByColumnId(datasetName, latest.copyNumber, info.id.underlying)
      case Truncated =>
        logTruncate(datasetName, latest.copyNumber)
        client.deleteFieldValuesByCopyNumber(datasetName, latest.copyNumber)
      case LastModifiedChanged(lm) =>
        // TODO : Support if-modified-since one day
      case ColumnCreated(info) =>
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
      case _: Any => // TODO - remove before committing!
    }

    // TODO : Don't do this. See notes above.
    Thread.sleep(1000) // scalastyle:ignore magic.number

    // Finally, get whatever the new latest copy is and bump its data version.
    val finalLatest = client.getLatestCopyForDataset(datasetName).getOrElse(
      throw new UnsupportedOperationException(s"Couldn't get latest copy number for dataset $datasetName"))
    client.updateDatasetCopyVersion(finalLatest.updateCopy(dataVersion))
  }

  private def checkDroppable(copy: DatasetCopy): Unit = {
    val droppableStages = Seq(LifecycleStage.Snapshotted, LifecycleStage.Unpublished)
    if (!droppableStages.contains(copy.stage)) {
      throw new UnsupportedOperationException(s"Cannot drop ${copy.stage} copy")
    }
    if (copy.copyNumber < 2) {
      throw new UnsupportedOperationException("Cannot drop initial copy")
    }
  }

  private def handleWorkingCopyCreate(datasetName: String, dataVersion: Long, events: Events): Events = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    if (wccEvents.hasNext) {
      wccEvents.next() match {
        case WorkingCopyCreated(copyInfo) =>
          // Make sure the copy we want to create doesn't already exist.
          val existingCopy = client.getDatasetCopy(datasetName, copyInfo.copyNumber)
          if (existingCopy.isDefined) {
            logger.info(s"dataset $datasetName copy ${copyInfo.copyNumber} already exists - resync!")
            throw new ResyncSecondaryException("Dataset copy already exists")
          } else {
            // Tell ES that this new copy exists
            logWorkingCopyCreated(datasetName, copyInfo.copyNumber)
            client.putDatasetCopy(datasetName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage)
          }
        case other: Event =>
          throw new UnsupportedOperationException(s"Unexpected event ${other.getClass}")
      }
    }

    if (wccEvents.hasNext) {
      throw new UnsupportedOperationException("Encountered >1 WorkingCopyCreated event in a single version")
    }

    remainingEvents
  }
}
