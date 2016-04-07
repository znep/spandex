package com.socrata.spandex.common

import com.socrata.spandex.common.client.{DatasetCopy, SearchResults}
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilder

trait ElasticsearchClientLogger extends Logging {
  def logClientConnected(transportAddress: InetSocketTransportAddress, settings: Settings): Unit =
    logger.debug(s"connected elasticsearch client at $transportAddress with ${settings.toDelimitedString(',')}")

  def logClientHealthcheckStatus(status: String): Unit =
    logger.debug(s"elasticsearch cluster healthcheck $status")

  def logIndexExistsRequest(index: String): Unit =
    logger.debug(s"does index '$index' exist?")

  def logIndexExistsResult(index: String, result: Boolean): Unit =
    logger.debug(s"index '$index' exists was '$result'")

  def logIndexCreateRequest(index: String, clusterName: String): Unit =
    logger.info("creating index {} on {}", index, clusterName)

  def logIndexAlreadyExists(index: String, clusterName: String): Unit =
    logger.info("actually that index ({}) already exists on cluster ({})", index, clusterName)

  def logBulkRequest(request: BulkRequestBuilder, refresh: Boolean): Unit =
    logger.debug(s"sending bulk request of size ${request.numberOfActions} with refresh=$refresh")

  def logDeleteByQueryRequest(queryBuilder: QueryBuilder, types: Seq[String], refresh: Boolean): Unit =
    logger.debug("delete by query {} on types={} with refresh={}",
      queryBuilder.toString.replaceAll("\\s+", " "), types.toString, refresh.toString)

  def logCopyFieldValuesRequest(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit =
    logger.debug(s"copy field_values from=$from to=$to refresh=$refresh")

  def logDatasetCopyIndexRequest(id: String, source: String): Unit =
    logger.debug(s"executing index dataset copy on id=$id with $source")

  def logDatasetCopyUpdateRequest(datasetCopy: DatasetCopy, source: String): Unit =
    logger.debug(s"executing update dataset copy version request on id=${datasetCopy.docId} with $source")

  def logDatasetCopySearchRequest(request: SearchRequestBuilder): Unit =
    logger.debug(s"executing elasticsearch search request ${request.toString.replaceAll("\\s+", " ")}")

  def logDatasetCopySearchResults(results: SearchResults[DatasetCopy]): Unit =
    logger.debug(s"received dataset copies ${results.thisPage.mkString(", ")}")

  def logDatasetCopyGetRequest(id: String): Unit =
    logger.debug(s"executing elasticsearch get request for dataset copy id = $id")

  def logDatasetCopyGetResult(result: Option[DatasetCopy]): Unit =
    logger.debug(s"received dataset copy $result")

  def logColumnMapIndexRequest(id: String, source: String): Unit =
    logger.debug(s"executing index column map on id=$id with $source")

  def logSearchResponse(response: SearchResponse): Unit =
    logger.debug(s"received {} hits out of {} total",
      response.getHits.hits.length.toString, response.getHits.totalHits.toString)
}
