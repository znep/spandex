package com.socrata.spandex.common.client

import com.socrata.spandex.common.ElasticSearchConfig
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import com.socrata.datacoordinator.secondary.LifecycleStage
import org.elasticsearch.action.index.IndexResponse
import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.metrics.max.Max
import ResponseExtensions._

class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) {
  private def byDatasetQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.datasetId, datasetId)
  private def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.datasetId, datasetId))
               .must(termQuery(SpandexFields.copyNumber, copyNumber))

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byDatasetQuery(datasetId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByDataset(datasetId: String): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.fieldValueMapping.mappingType)
          .setQuery(byDatasetQuery(datasetId))
          .execute.actionGet

  def searchFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.fieldValueMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet

  def putDatasetCopy(datasetId: String, copyNumber: Long, dataVersion: Long, stage: LifecycleStage): IndexResponse = {
    val id = s"$datasetId|$copyNumber"
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    client.prepareIndex(config.index, config.datasetCopyMapping.mappingType, id)
          .setSource(source)
          .execute.actionGet
  }

  def getLatestCopyNumberForDataset(datasetId: String): Long = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.datasetCopyMapping.mappingType)
                         .setQuery(byDatasetQuery(datasetId))
                         .addAggregation(max("latest_copy").field(SpandexFields.copyNumber))
                         .execute.actionGet
    val map = response.getAggregations.asMap()
    if (map.size == 0 || !map.containsKey("latest_copy")) 0
    else {
      val latest = map.get("latest_copy").asInstanceOf[Max].getValue.toLong
      if (latest >=0) latest else 0
    }
  }

  def getDatasetCopy(datasetId: String, copyNumber: Long): Option[DatasetCopy] = {
    val id = s"$datasetId|$copyNumber"
    val response = client.prepareGet(config.index, config.datasetCopyMapping.mappingType, id)
                         .execute.actionGet
    response.result[DatasetCopy]
  }

  def searchCopiesByDataset(datasetId: String): SearchResults[DatasetCopy] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.datasetCopyMapping.mappingType)
                         .setQuery(byDatasetQuery(datasetId))
                         .execute.actionGet
    response.results[DatasetCopy]
  }

  def deleteDatasetCopy(datasetId: String, copyNumber: Long): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.datasetCopyMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet
}
