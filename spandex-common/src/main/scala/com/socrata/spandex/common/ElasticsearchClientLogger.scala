package com.socrata.spandex.common

import com.socrata.spandex.common.client.{DatasetCopy, SearchResults}
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilder

trait ElasticsearchClientLogger extends Logging {
  def logIndexExistsRequest(index: String): Unit = {
    logger.debug(s"does index '$index' exist?")
  }

  def logIndexExistsResult(index: String, result: Boolean): Unit = {
    logger.debug(s"index '$index' exists was '$result'")
  }

  def logBulkRequest(request: BulkRequestBuilder, refresh: Boolean): Unit = {
    logger.debug(s"sending bulk request of size ${request.numberOfActions} with refresh=$refresh")
  }

  def logDeleteByQueryRequest(queryBuilder: QueryBuilder, types: Seq[String], refresh: Boolean): Unit = {
    val requestString = queryBuilder.toString.replaceAll("\\s+", " ")
    logger.debug(s"delete by query $requestString on types=$types with refresh=$refresh")
  }

  def logCopyFieldValuesRequest(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit = {
    logger.debug(s"copy field_values from=$from to=$to refresh=$refresh")
  }

  def logDatasetCopyIndexRequest(id: String, source: String): Unit = {
    logger.debug(s"executing index dataset copy on id=$id with $source")
  }

  def logDatasetCopyUpdateRequest(datasetCopy: DatasetCopy, source: String): Unit = {
    logger.debug(s"executing update dataset copy version request on id=${datasetCopy.docId} with $source")
  }

  def logDatasetCopySearchRequest(request: SearchRequestBuilder): Unit = {
    logger.debug(s"executing elasticsearch search request ${request.toString.replaceAll("\\s+", " ")}")
  }

  def logDatasetCopySearchResults(results: SearchResults[DatasetCopy]): Unit = {
    logger.debug(s"received dataset copies ${results.thisPage.mkString(", ")}")
  }

  def logDatasetCopyGetRequest(id: String): Unit = {
    logger.debug(s"executing elasticsearch get request for dataset copy id = $id")
  }

  def logDatasetCopyGetResult(result: Option[DatasetCopy]): Unit = {
    logger.debug(s"received dataset copy $result")
  }
}
