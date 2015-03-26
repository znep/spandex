package com.socrata.spandex.common.client

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common.ElasticSearchConfig
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.index.{IndexResponse, IndexRequestBuilder}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.update.{UpdateResponse, UpdateRequestBuilder}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort.SortOrder
import ResponseExtensions._

case class ElasticSearchResponseFailed(msg: String) extends Exception(msg)

// scalastyle:off number.of.methods
class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) {
  private def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.DatasetId, datasetId)
  private def byDatasetIdAndStageQuery(datasetId: String, stage: LifecycleStage): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.Stage, stage.toString))
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

  private def checkForFailures(response: ActionResponse): Unit = response match {
    case i: IndexResponse =>
      if (!i.isCreated) throw ElasticSearchResponseFailed(s"Document ${i.getId} was not successfully indexed")
    case u: UpdateResponse =>
      // No op - UpdateResponse doesn't have any useful state to check
    case d: DeleteResponse =>
      // No op - we don't care to throw an exception if d.isFound is false,
      // since that means the document is effectively deleted.
    case dbq: DeleteByQueryResponse =>
      val failures = dbq.getIndex(config.index).getFailures
      if (failures.nonEmpty) {
        throw ElasticSearchResponseFailed(s"DeleteByQuery response contained failures: " +
          failures.map(_.reason).mkString(","))
      }
    case b: BulkResponse =>
      if (b.hasFailures) {
        throw new ElasticSearchResponseFailed(s"Bulk response contained failures: " +
          b.buildFailureMessage())
      }
    case r: ActionResponse =>
      throw new NotImplementedError(s"Haven't implemented failure check for ${r.getClass.getSimpleName}")
  }

  def indexExists: Boolean = {
    val request = client.admin().indices().exists(new IndicesExistsRequest(config.index))
    request.actionGet().isExists
  }

  def putColumnMap(columnMap: ColumnMap): Unit =
    checkForFailures(
      client.prepareIndex(config.index, config.columnMapMapping.mappingType, columnMap.docId)
          .setSource(JsonUtil.renderJson(columnMap))
          .execute.actionGet)

  def getColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val response = client.prepareGet(config.index, config.columnMapMapping.mappingType, id)
                         .execute.actionGet
    response.result[ColumnMap]
  }

  def deleteColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Unit = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    checkForFailures(client.prepareDelete(config.index, config.columnMapMapping.mappingType, id)
                           .execute.actionGet)
  }

  def getFieldValue(fieldValue: FieldValue): Option[FieldValue] = {
    val response = client.prepareGet(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
                         .execute.actionGet
    response.result[FieldValue]
  }

  def indexFieldValue(fieldValue: FieldValue): Unit =
    checkForFailures(getIndexRequest(fieldValue).execute.actionGet)

  def updateFieldValue(fieldValue: FieldValue): Unit =
    checkForFailures(getUpdateRequest(fieldValue).execute.actionGet)

  def getIndexRequest(fieldValue: FieldValue) : IndexRequestBuilder =
    client.prepareIndex(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setSource(JsonUtil.renderJson(fieldValue))

  def getUpdateRequest(fieldValue: FieldValue): UpdateRequestBuilder = {
    client.prepareUpdate(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setDocAsUpsert(true)
          .setDoc(s"""{ value : "${fieldValue.value}" }""")
  }

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Seq[Any]): Unit =
    checkForFailures(requests.foldLeft(client.prepareBulk()) { case (bulk, single) =>
        single match {
          case i: IndexRequestBuilder => bulk.add(i)
          case u: UpdateRequestBuilder => bulk.add(u)
          case a: Any =>
            throw new UnsupportedOperationException(
              s"Bulk requests with ${a.getClass.getSimpleName} not supported")
        }
      }.execute.actionGet)

  def copyFieldValues(from: DatasetCopy, to: DatasetCopy): Unit = {
    val timeout = new TimeValue(config.dataCopyTimeout)
    val scrollInit = client.prepareSearch(config.index)
                           .setTypes(config.fieldValueMapping.mappingType)
                           .setQuery(byCopyNumberQuery(from.datasetId, from.copyNumber))
                           .setSearchType(SearchType.SCAN)
                           .setScroll(timeout)
                           .setSize(config.dataCopyBatchSize)
                           .execute.actionGet

    var done = false
    while (!done) {
      val response = client.prepareSearchScroll(scrollInit.getScrollId)
                           .setScroll(timeout)
                           .execute.actionGet

      val batch = response.results[FieldValue].thisPage.map { src =>
        getIndexRequest(FieldValue(src.datasetId, to.copyNumber, src.columnId, src.rowId, src.value))
      }

      if (batch.isEmpty) {
        done = true
      } else {
        sendBulkRequest(batch)
      }
    }
  }

  def searchFieldValuesByDataset(datasetId: String): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byDatasetIdQuery(datasetId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByDataset(datasetId: String): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.fieldValueMapping.mappingType)
                           .setQuery(byDatasetIdQuery(datasetId))
                           .execute.actionGet)

  def searchFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.fieldValueMapping.mappingType)
                           .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                           .execute.actionGet)

  def searchFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.fieldValueMapping.mappingType)
                           .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
                           .execute.actionGet)

  def searchFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): SearchResults[FieldValue] = {
    val response = client.prepareSearch(config.index)
                         .setTypes(config.fieldValueMapping.mappingType)
                         .setQuery(byRowIdQuery(datasetId, copyNumber, rowId))
                         .execute.actionGet
    response.results[FieldValue]
  }

  def deleteFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.fieldValueMapping.mappingType)
                           .setQuery(byColumnIdQuery(datasetId, copyNumber, columnId))
                           .execute.actionGet)

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
    checkForFailures(
      client.prepareUpdate(config.index, config.datasetCopyMapping.mappingType, id)
            .setDoc(source)
            .setUpsert()
            .execute.actionGet)
  }

  def getLatestCopyForDataset(datasetId: String,
                              publishedOnly: Boolean = false): Option[DatasetCopy] = {
    val latestCopyPlaceholder = "latest_copy"
    val query =
      if (publishedOnly) {
        byDatasetIdAndStageQuery(datasetId, LifecycleStage.Published)
      } else {
        byDatasetIdQuery(datasetId)
      }

    val response = client.prepareSearch(config.index)
                         .setTypes(config.datasetCopyMapping.mappingType)
                         .setQuery(query)
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
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.datasetCopyMapping.mappingType)
                           .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                           .execute.actionGet)
}
