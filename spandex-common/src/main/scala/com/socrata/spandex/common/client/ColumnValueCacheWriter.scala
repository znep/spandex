package com.socrata.spandex.common.client

import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue

import com.typesafe.scalalogging.slf4j.Logging

/**
  * The ColumnValueCacheWriterThread writes values cached from its provided spandexESClient to Elasticsearch.
  *
  * @param spandexESClient          The Elasticsearch Client that values will be written to ES from
  * @param columnValuesCache        A reference to the column value cache in spandexESClient
  * @param flushCacheTimeoutSeconds The maximum amount of time between writes... if we don't have enough Column
  *                                 Values to justify a flush for flushCacheTimeoutSeconds seconds, we write to
  *                                 the cache regardless.
  * @param sleepIntervalMillis      The amount of time we sleep at the end of the thread's main loop
  */
class ColumnValueCacheWriter(
    val spandexESClient: SpandexElasticSearchClient,
    val shouldKill : java.util.ArrayList[Boolean],
    val columnValuesCache: ArrayBlockingQueue[ColumnValue],
    val flushCacheTimeoutSeconds: Int = 30, // scalastyle:off magic.number
    val sleepIntervalMillis: Int = 250 // scalastyle:off magic.number
) extends Runnable with Logging {
  def run: Unit = {
    logger.info("ColumnValueWriterThread thread initialized")
    var lastWriteTime = Instant.now()
    while (shouldKill.isEmpty) {
      if (columnValuesCache.size > spandexESClient.dataCopyBatchSize) {
        spandexESClient.flushColumnValueCache(spandexESClient.dataCopyBatchSize)
        lastWriteTime = Instant.now()
      }
      /* If we haven't written anything in 30 seconds, go ahead and flush the cache */
      if (lastWriteTime.plusSeconds(flushCacheTimeoutSeconds).isBefore(Instant.now())) {
        spandexESClient.flushColumnValueCache()
        lastWriteTime = Instant.now()
      }

      Thread.sleep(sleepIntervalMillis)
    }
    logger.info("ColumnValueWriterThread shutting down")
  }
}
