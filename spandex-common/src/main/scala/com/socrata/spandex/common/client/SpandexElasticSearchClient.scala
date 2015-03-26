package com.socrata.spandex.common.client

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common.ElasticSearchConfig
import org.elasticsearch.action.{ActionResponse, ActionRequest, ActionRequestBuilder}
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.update.UpdateRequestBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort.SortOrder
import ResponseExtensions._

// TODO : Validate the response codes of methods that return Unit

class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) {
  private def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.DatasetId, datasetId)
  private def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
  private def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
               .must(termQuery(SpandexFields.ColumnId, columnId))
  private def byRowIdQuery(datasetId: String, copyNumber: Long, rowId: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
               .must(termQuery(SpandexFields.RowId, rowId))

  def indexExists: Boolean = {
    val request = client.admin().indices().exists(new IndicesExistsRequest(config.index))
    request.actionGet().isExists
  }

  def putColumnMap(columnMap: ColumnMap): Unit = {
    val source = JsonUtil.renderJson(columnMap)
    client.prepareIndex(config.index, config.columnMapMapping.mappingType, columnMap.docId)
          .setSource(source)
          .execute.actionGet
  }

  def getColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val response = client.prepareGet(config.index, config.columnMapMapping.mappingType, id)
                         .execute.actionGet
    response.result[ColumnMap]
  }

  def deleteColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Unit = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    client.prepareDelete(config.index, config.columnMapMapping.mappingType, id)
          .execute.actionGet
  }

  def getFieldValue(fieldValue: FieldValue): Option[FieldValue] = {
    val response = client.prepareGet(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
                         .execute.actionGet
    response.result[FieldValue]
  }

  def indexFieldValue(fieldValue: FieldValue): Unit =
    getIndexRequest(fieldValue).execute.actionGet

  def updateFieldValue(fieldValue: FieldValue): Unit =
    getUpdateRequest(fieldValue).execute.actionGet

  def getIndexRequest(fieldValue: FieldValue) : IndexRequestBuilder = {
    val source = JsonUtil.renderJson(fieldValue)
    client.prepareIndex(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setSource(source)
  }

  def getUpdateRequest(fieldValue: FieldValue): UpdateRequestBuilder = {
    client.prepareUpdate(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setDocAsUpsert(true)
          .setDoc(s"""{ value : "${fieldValue.value}" }""")
  }

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Seq[Any]): Unit =
    requests.foldLeft(client.prepareBulk()) { case (bulk, single) =>
      single match {
        case i: IndexRequestBuilder => bulk.add(i)
        case u: UpdateRequestBuilder => bulk.add(u)
        case a: Any =>
          throw new UnsupportedOperationException(
            s"Bulk requests with ${a.getClass.getSimpleName} not supported")
      }
    }.execute.actionGet

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByDataset(datasetId: String): Unit =
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

  def deleteFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.fieldValueMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet

  def searchFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): Unit =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.fieldValueMapping.mappingType)
          .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
          .execute.actionGet

  def searchFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): Unit =
    client.prepareDeleteByQuery(config.index)
      .setTypes(config.fieldValueMapping.mappingType)
      .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
      .execute.actionGet

  def putDatasetCopy(datasetId: String, copyNumber: Long, dataVersion: Long, stage: LifecycleStage): Unit = {
    val id = s"$datasetId|$copyNumber"
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    client.prepareIndex(config.index, config.datasetCopyMapping.mappingType, id)
          .setSource(source)
          .execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy): Unit = {
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
                         .addSort(SpandexFields.CopyNumber, SortOrder.DESC)
                         .addAggregation(max(latestCopyPlaceholder).field(SpandexFields.CopyNumber))
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

  def deleteDatasetCopy(datasetId: String, copyNumber: Long): Unit =
    client.prepareDeleteByQuery(config.index)
          .setTypes(config.datasetCopyMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet
}
