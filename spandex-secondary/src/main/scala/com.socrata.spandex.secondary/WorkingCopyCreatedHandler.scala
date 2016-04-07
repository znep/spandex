package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.{ResyncSecondaryException, WorkingCopyCreated}
import com.socrata.spandex.common.client.SpandexElasticSearchClient

case class WorkingCopyCreatedHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def go(datasetName: String, dataVersion: Long, events: Iterator[Event]): Iterator[Event] = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    if (wccEvents.hasNext) {
      val event = wccEvents.next()
      event match {
        case WorkingCopyCreated(copyInfo) =>
          // Make sure the copy we want to create doesn't already exist.
          val existingCopy = client.datasetCopy(datasetName, copyInfo.copyNumber)
          if (existingCopy.isDefined) {
            logResync(datasetName, copyInfo.copyNumber)
            throw new ResyncSecondaryException("Dataset copy already exists")
          } else {
            // Tell ES that this new copy exists
            logWorkingCopyCreated(datasetName, copyInfo.copyNumber)
            client.putDatasetCopy(
              datasetName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage, refresh = true)
          }
        case _ =>
          throw new UnsupportedOperationException(s"Unexpected event ${event.getClass}")
      }
    }

    if (wccEvents.hasNext) {
      throw new UnsupportedOperationException("Encountered >1 WorkingCopyCreated event in a single version")
    }

    remainingEvents
  }
}
