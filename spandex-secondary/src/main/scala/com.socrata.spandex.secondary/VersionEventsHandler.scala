package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common.client.SpandexElasticSearchClient

class VersionEventsHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def handle(datasetInfo: DatasetInfo, // scalastyle:ignore cyclomatic.complexity
             dataVersion: Long,
             events: Events): Unit = {
    require(dataVersion > 0, s"Unexpected value for data version: $dataVersion")

    // First, handle any working copy events
    val remainingEvents = handleWorkingCopyCreate(datasetInfo, dataVersion, events)

    // Find the latest dataset copy number
    val latest = client.getLatestCopyNumberForDataset(datasetInfo.internalName)

    // Now handle everything else
    remainingEvents.foreach {
      case Truncated =>
        logTruncate(datasetInfo.internalName, latest)
        client.deleteFieldValuesByCopyNumber(datasetInfo.internalName, latest)
      case LastModifiedChanged(lm) =>
        // TODO : Support if-modified-since one day
      case ColumnCreated(info) =>
      case RowIdentifierSet(info) =>
      case RowIdentifierCleared(info) =>
      case SystemRowIdentifierChanged(info) =>
      case VersionColumnChanged(info) =>
      case RollupCreatedOrUpdated(info) =>
      case RollupDropped(info) =>
        // These events don't result in a copy bump or changed data, so no-op
      case WorkingCopyCreated(info) =>
        // We have handled all WorkingCopyCreated events above. This should never happen.
        throw new UnsupportedOperationException("Unexpected WorkingCopyCreated event")
      case _: Any => // TODO - remove before committing!
    }
  }

  private def handleWorkingCopyCreate(datasetInfo: DatasetInfo, dataVersion: Long, events: Events): Events = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    if (wccEvents.hasNext) {
      wccEvents.next() match {
        case WorkingCopyCreated(copyInfo) =>
          // Make sure the copy we want to create doesn't already exist.
          val existingCopy = client.getDatasetCopy(datasetInfo.internalName, copyInfo.copyNumber)
          if (existingCopy.isDefined) {
            logger.info(s"dataset ${datasetInfo.internalName} copy ${copyInfo.copyNumber} already exists - resync!")
            throw new ResyncSecondaryException("Dataset copy already exists")
          } else {
            // Tell ES that this new copy exists
            logWorkingCopyCreated(datasetInfo.internalName, copyInfo.copyNumber)
            client.putDatasetCopy(datasetInfo.internalName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage)
          }
        case other: Event[_, _] =>
          throw new UnsupportedOperationException(s"Unexpected event ${other.getClass}")
      }
    }

    if (wccEvents.hasNext) {
      throw new UnsupportedOperationException("Encountered >1 WorkingCopyCreated event in a single version")
    }

    remainingEvents
  }
}
