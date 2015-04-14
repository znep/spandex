package com.socrata.spandex.secondary

import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.socrata.datacoordinator.secondary.{ResyncSecondaryException, WorkingCopyCreated}

case class WorkingCopyCreatedHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def go(datasetName: String, dataVersion: Long, events: Iterator[Event]): Iterator[Event] = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    if (wccEvents.hasNext) {
      wccEvents.next() match {
        case WorkingCopyCreated(copyInfo) =>
          // Make sure the copy we want to create doesn't already exist.
          val existingCopy = client.datasetCopy(datasetName, copyInfo.copyNumber)
          if (existingCopy.isDefined) {
            logger.info(s"dataset $datasetName copy ${copyInfo.copyNumber} already exists - resync!")
            throw new ResyncSecondaryException("Dataset copy already exists")
          } else {
            // Tell ES that this new copy exists
            logWorkingCopyCreated(datasetName, copyInfo.copyNumber)
            client.putDatasetCopy(
              datasetName, copyInfo.copyNumber, dataVersion, copyInfo.lifecycleStage, refresh = true)
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
