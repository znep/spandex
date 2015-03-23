package com.socrata.spandex.common.client

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.ElasticSearchConfig
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort.SortOrder
import ResponseExtensions._

class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) {
  private def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.datasetId, datasetId)
  private def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.datasetId, datasetId))
               .must(termQuery(SpandexFields.copyNumber, copyNumber))
  private def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: String): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.datasetId, datasetId))
               .must(termQuery(SpandexFields.copyNumber, copyNumber))
               .must(termQuery(SpandexFields.columnId, columnId))

  def indexExists: Boolean = {
    val request = client.admin().indices().exists(new IndicesExistsRequest(config.index))
    request.actionGet().isExists
  }

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByDataset(datasetId: String): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.fieldValueMapping.mappingType)
          .setQuery(byDatasetIdQuery(datasetId))
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

  def searchFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: String): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
      .setTypes(config.fieldValueMapping.mappingType)
      .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
      .execute.actionGet

  def putDatasetCopy(datasetId: String, copyNumber: Long, dataVersion: Long, stage: LifecycleStage): IndexResponse = {
    val id = s"$datasetId|$copyNumber"
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    client.prepareIndex(config.index, config.datasetCopyMapping.mappingType, id)
          .setSource(source)
          .execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy): UpdateResponse = {
    val id = s"${datasetCopy.datasetId}|${datasetCopy.copyNumber}"
    val source = JsonUtil.renderJson(datasetCopy)
    client.prepareUpdate(config.index, config.datasetCopyMapping.mappingType, id)
          .setDoc(source)
          .setUpsert()
          .execute.actionGet
  }

  def getLatestCopyForDataset(datasetId: String): Option[DatasetCopy] = {
    val latestCopyPlaceholder = "latest_copy"
    val response = client.prepareSearch(config.index)
                         .setTypes(config.datasetCopyMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .setSize(1)
                         .addSort(SpandexFields.copyNumber, SortOrder.DESC)
                         .addAggregation(max(latestCopyPlaceholder).field(SpandexFields.copyNumber))
                         .execute.actionGet
    val results = response.results[DatasetCopy]
    results.thisPage.headOption
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
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[DatasetCopy]
  }

  def deleteDatasetCopy(datasetId: String, copyNumber: Long): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.datasetCopyMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet
}
