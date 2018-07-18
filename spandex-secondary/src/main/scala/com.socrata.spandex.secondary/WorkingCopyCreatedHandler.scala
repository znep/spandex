package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.{ResyncSecondaryException, WorkingCopyCreated}
import com.socrata.soql.types.{SoQLType, SoQLValue}
import com.socrata.spandex.common.client.{DatasetCopy, Eventually, RefreshPolicy, SpandexElasticSearchClient}

class WorkingCopyCreatedHandler(
    client: SpandexElasticSearchClient,
    refresh: RefreshPolicy = Eventually)
  extends SecondaryEventLogger {

  def go(datasetName: String, dataVersion: Long, events: Iterator[Event]): Iterator[Event] = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(_) => true
      case _ => false
    }

    if (wccEvents.hasNext) {
      wccEvents.next() match {
        case WorkingCopyCreated(copyInfo) =>
          // Make sure the copy we want to create doesn't already exist.
          client.datasetCopy(datasetName, copyInfo.copyNumber).foreach { existingCopy =>
            throw new ResyncSecondaryException(s"Dataset copy already exists: $existingCopy")
          }

          logWorkingCopyCreated(datasetName, copyInfo.copyNumber)
          Some(client.putDatasetCopy(datasetName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage, refresh))
        case event: Event =>
          throw new UnsupportedOperationException(s"Unexpected event ${event.getClass}")
      }
    }

    if (wccEvents.hasNext) {
      throw new UnsupportedOperationException("Encountered >1 WorkingCopyCreated event in a single version")
    }

    remainingEvents
  }
}
