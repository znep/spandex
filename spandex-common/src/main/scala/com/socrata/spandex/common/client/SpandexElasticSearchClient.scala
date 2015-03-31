package com.socrata.spandex.common.client

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common._
import com.socrata.spandex.common.client.ResponseExtensions._
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.action.index.{IndexRequestBuilder, IndexResponse}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.update.{UpdateRequestBuilder, UpdateResponse}
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder

// scalastyle:off number.of.methods
case class ElasticSearchResponseFailed(msg: String) extends Exception(msg)

// Setting refresh to true on ES write calls, because we always want to be
// sure that we're operating on the very latest copy. We are trading off some speed to get
// consistency.
// Without this, when we create a new working copy, more often than not the brand new
// dataset_copy document isn't indexed yet, and we perform all subsequent event operations
// on a stale copy.
// Caveats:
// - refresh=true only guarantees consistency on a single shard.
// - We aren't actually sure what the perf implications of running like this at production scale are.
// http://www.elastic.co/guide/en/elasticsearch/reference/1.x/docs-index_.html#index-refresh
class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) with Logging {
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
      if (!i.isCreated) throw ElasticSearchResponseFailed(
        s"${i.getType} doc with id ${i.getId} was not successfully indexed")
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

  def refresh(): Unit = client.admin().indices().prepareRefresh(config.index).execute.actionGet

  def indexExists: Boolean = {
    val request = client.admin().indices().exists(new IndicesExistsRequest(config.index))
    request.actionGet().isExists
  }

  def putColumnMap(columnMap: ColumnMap, refresh: Boolean): Unit =
    checkForFailures(
      client.prepareIndex(config.index, config.columnMapMapping.mappingType, columnMap.docId)
          .setSource(JsonUtil.renderJson(columnMap))
          .setRefresh(refresh)
          .execute.actionGet)

  def getColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val response = client.prepareGet(config.index, config.columnMapMapping.mappingType, id)
                         .execute.actionGet
    response.result[ColumnMap]
  }

  def getColumnMap(datasetId: String, copyNumber: Long, systemColumnId: Long): Option[ColumnMap] = {
    val response = client.prepareSearch(config.index)
      .setTypes(config.columnMapMapping.mappingType)
      .setQuery(boolQuery().must(termQuery(SpandexFields.ColumnId, systemColumnId)))
      .execute.actionGet
    response.results[ColumnMap].thisPage.headOption
  }

  def deleteColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Unit = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    checkForFailures(client.prepareDelete(config.index, config.columnMapMapping.mappingType, id)
                           .execute.actionGet)
  }

  def searchColumnMapsByDataset(datasetId: String): SearchResults[ColumnMap] =
    client.prepareSearch(config.index)
          .setTypes(config.columnMapMapping.mappingType)
          .setQuery(byDatasetIdQuery(datasetId))
          .execute.actionGet.results[ColumnMap]

  def deleteColumnMapsByDataset(datasetId: String): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.columnMapMapping.mappingType)
                           .setQuery(byDatasetIdQuery(datasetId))
                           .execute.actionGet)

  def searchColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(config.index)
          .setTypes(config.columnMapMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .execute.actionGet.results[ColumnMap]

  def deleteColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    checkForFailures(client.prepareDeleteByQuery(config.index)
                           .setTypes(config.columnMapMapping.mappingType)
                           .setQuery(byCopyNumberQuery(datasetId, copyNumber))
                           .execute.actionGet)

  def getFieldValue(fieldValue: FieldValue): Option[FieldValue] = {
    val response = client.prepareGet(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
                         .execute.actionGet
    response.result[FieldValue]
  }

  def indexFieldValue(fieldValue: FieldValue, refresh: Boolean): Unit =
    checkForFailures(getIndexRequest(fieldValue).setRefresh(refresh).execute.actionGet)

  def updateFieldValue(fieldValue: FieldValue, refresh: Boolean): Unit =
    checkForFailures(getUpdateRequest(fieldValue).setRefresh(refresh).execute.actionGet)

  def getIndexRequest(fieldValue: FieldValue) : IndexRequestBuilder =
    client.prepareIndex(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setSource(JsonUtil.renderJson(fieldValue))

  def getUpdateRequest(fieldValue: FieldValue): UpdateRequestBuilder = {
    client.prepareUpdate(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setDocAsUpsert(true)
          .setDoc(s"""{ value : "${fieldValue.value}" }""")
  }

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Seq[Any], refresh: Boolean): Unit = {
    if (requests.nonEmpty) {
      val baseRequest = client.prepareBulk().setRefresh(refresh)
      checkForFailures(requests.foldLeft(baseRequest) { case (bulk, single) =>
        single match {
          case i: IndexRequestBuilder => bulk.add(i)
          case u: UpdateRequestBuilder => bulk.add(u)
          case a: Any =>
            throw new UnsupportedOperationException(
              s"Bulk requests with ${a.getClass.getSimpleName} not supported")
        }
      }.execute.actionGet)
    }
  }

  def copyFieldValues(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit = {
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
        sendBulkRequest(batch, refresh)
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

  def putDatasetCopy(datasetId: String,
                     copyNumber: Long,
                     dataVersion: Long,
                     stage: LifecycleStage,
                     refresh: Boolean): Unit = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    client.prepareIndex(config.index, config.datasetCopyMapping.mappingType, id)
          .setSource(source)
          .setRefresh(refresh)
          .execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy, refresh: Boolean): Unit = {
    val source = JsonUtil.renderJson(datasetCopy)
    checkForFailures(
      client.prepareUpdate(config.index, config.datasetCopyMapping.mappingType, datasetCopy.docId)
            .setDoc(source)
            .setUpsert()
            .setRefresh(refresh)
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
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
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

  def deleteDatasetCopiesByDataset(datasetId: String): DeleteByQueryResponse =
    client.prepareDeleteByQuery(config.index)
      .setTypes(config.datasetCopyMapping.mappingType)
      .setQuery(byDatasetIdQuery(datasetId))
      .execute.actionGet

  def getSuggestions(column: ColumnMap, text: String, fuzz: Fuzziness, size: Int): Suggest = {
    val suggestion = new CompletionSuggestionFuzzyBuilder("suggest")
      .addContextField(SpandexFields.CompositeId, column.composideId)
      .setFuzziness(fuzz)
      .field(SpandexFields.Value)
      .text(text)
      .size(size)

    val response = client
      .prepareSuggest(config.index)
      .addSuggestion(suggestion)
      .execute().actionGet()

    response.getSuggest
  }
}
