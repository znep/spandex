package com.socrata.spandex.secondary

import com.typesafe.scalalogging.slf4j.Logging

trait SecondaryEventLogger extends Logging {
  private def logEvent(eventName: String, description: String): Unit =
    logger.info(s"|$eventName| event: $description")

  def logWorkingCopyCreated(dataset: String, copy: Long): Unit =
    logEvent("WorkingCopyCreated",
             s"registering new copy number $copy for dataset $dataset")

  def logTruncate(dataset: String, copy: Long): Unit =
    logEvent("Truncate",
             s"deleting field values for latest copy $copy of dataset $dataset")
}
