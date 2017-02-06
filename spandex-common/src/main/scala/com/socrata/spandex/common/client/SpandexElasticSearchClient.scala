package com.socrata.spandex.common.client

import scala.collection.mutable
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import com.socrata.spandex.common._
import com.socrata.spandex.common.client.ResponseExtensions._
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.{DeleteRequestBuilder, DeleteResponse}
import org.elasticsearch.action.index.{IndexRequestBuilder, IndexResponse}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.action.update.{UpdateRequestBuilder, UpdateResponse}
import org.elasticsearch.common.unit.{Fuzziness, TimeValue}
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.aggregations.AggregationBuilders._
import org.elasticsearch.search.aggregations.bucket.terms.Terms
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
class SpandexElasticSearchClient(config: ElasticSearchConfig) extends ElasticSearchClient(config) {
  protected def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.DatasetId, datasetId)
  protected def byDatasetIdAndStageQuery(datasetId: String, stage: LifecycleStage): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.Stage, stage.toString))
  protected def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
  protected def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
               .must(termQuery(SpandexFields.ColumnId, columnId))
  protected def byColumnCompositeId(column: ColumnMap): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.CompositeId, column.compositeId))
  protected def byRowIdQuery(datasetId: String, copyNumber: Long, rowId: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.DatasetId, datasetId))
               .must(termQuery(SpandexFields.CopyNumber, copyNumber))
               .must(termQuery(SpandexFields.RowId, rowId))

  private[this] def checkForFailures(response: ActionResponse): Unit = response match {
    case i: IndexResponse =>
      if (!i.isCreated) throw ElasticSearchResponseFailed(
        s"${i.getType} doc with id ${i.getId} was not successfully indexed")
    case u: UpdateResponse =>
    // No op - UpdateResponse doesn't have any useful state to check
    case d: DeleteResponse =>
    // No op - we don't care to throw an exception if d.isFound is false,
    // since that means the document is effectively deleted.
    case b: BulkResponse =>
      if (b.hasFailures) {
        throw new ElasticSearchResponseFailed(s"Bulk response contained failures: " +
          b.buildFailureMessage())
      }
    case _ =>
      throw new NotImplementedError(s"Haven't implemented failure check for ${response.getClass.getSimpleName}")
  }

  def refresh(): Unit = client.admin().indices().prepareRefresh(config.index).execute.actionGet

  def indexExists: Boolean = {
    logIndexExistsRequest(config.index)
    val request = client.admin.indices.exists(new IndicesExistsRequest(config.index))
    val result = request.actionGet.isExists
    logIndexExistsResult(config.index, result)
    result
  }

  def putColumnMap(columnMap: ColumnMap, refresh: Boolean): Unit = {
    val source = JsonUtil.renderJson(columnMap)
    val request = client.prepareIndex(config.index, config.columnMapMapping.mappingType, columnMap.docId)
      .setSource(source)
      .setRefresh(refresh)
    logColumnMapIndexRequest(columnMap.docId, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  def columnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
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

  def deleteColumnMapsByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(config.columnMapMapping.mappingType))

  // We don't expect the number of column maps to exceed config.dataCopyBatchSize.
  // As of April 2015 the widest dataset is ~1000 cols wide.
  def searchLotsOfColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(config.index)
          .setTypes(config.columnMapMapping.mappingType)
          .setQuery(byCopyNumberQuery(datasetId, copyNumber))
          .setSize(config.dataCopyBatchSize)
          .execute.actionGet.results[ColumnMap]

  def deleteColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(config.columnMapMapping.mappingType))

  def indexFieldValue(fieldValue: FieldValue, refresh: Boolean): Unit =
    checkForFailures(fieldValueIndexRequest(fieldValue).setRefresh(refresh).execute.actionGet)

  def updateFieldValue(fieldValue: FieldValue, refresh: Boolean): Unit =
    checkForFailures(fieldValueUpdateRequest(fieldValue).setRefresh(refresh).execute.actionGet)

  def fieldValueIndexRequest(fieldValue: FieldValue): IndexRequestBuilder =
    client.prepareIndex(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setSource(JsonUtil.renderJson(fieldValue))

  def fieldValueUpdateRequest(fieldValue: FieldValue): UpdateRequestBuilder = {
    client.prepareUpdate(config.index, config.fieldValueMapping.mappingType, fieldValue.docId)
          .setDoc(JsonUtil.renderJson(fieldValue))
          .setUpsert()
  }

  def fieldValueDeleteRequest(datasetId: String,
                              copyNumber: Long,
                              columnId: Long,
                              rowId: Long): DeleteRequestBuilder = {
    val docId = FieldValue.makeDocId(datasetId, copyNumber, columnId, rowId)
    client.prepareDelete(config.index, config.fieldValueMapping.mappingType, docId)
  }

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Seq[Any], refresh: Boolean): Option[BulkResponse] = {
    if (requests.nonEmpty) {
      val baseRequest = client.prepareBulk().setRefresh(refresh)
      val bulkRequest = requests.foldLeft(baseRequest) { case (bulk, single) =>
        single match {
          case i: IndexRequestBuilder => bulk.add(i)
          case u: UpdateRequestBuilder => bulk.add(u)
          case d: DeleteRequestBuilder => bulk.add(d)
          case _ =>
            throw new UnsupportedOperationException(
              s"Bulk requests with ${single.getClass.getSimpleName} not supported")
        }
      }
      logBulkRequest(bulkRequest, refresh)
      val bulkResponse = bulkRequest.execute.actionGet
      checkForFailures(bulkResponse)
      Some(bulkResponse)
    } else { None }
  }

  /**
    * Scan + scroll response size is per shard, so we calculate the desired total size after counting primary shards.
    * See https://www.elastic.co/guide/en/elasticsearch/guide/1.x/scan-scroll.html#id-1.4.11.10.11.3
    *
    * Note: the value `config.index` could be an alias, and it's possible to iterate over returned settings and find
    * a concrete index with corresponding alias. But *index* vs *total* primary shards is almost the same number.
    */
  private def calculateScrollSize(desiredSize: Int): Int = {
    val clusterHealthRequest = client.admin.cluster.prepareHealth()
    val clusterHealthResponse = clusterHealthRequest.execute.actionGet
    val primaryShardCount = clusterHealthResponse.getActivePrimaryShards
    desiredSize / primaryShardCount
  }

  def deleteByQuery(queryBuilder: QueryBuilder, types: Seq[String] = Nil, refresh: Boolean = true): Map[String, Int] = {
    logDeleteByQueryRequest(queryBuilder, types, refresh)
    val timeout = new TimeValue(config.dataCopyTimeout)
    val scrollInitRequest = client.prepareSearch(config.index)
      .setQuery(queryBuilder)
      .setNoFields() // for delete request: only interested in type and id
      .setTypes(types: _*)
      .setSearchType(SearchType.SCAN)
      .setScroll(timeout)
      .setSize(calculateScrollSize(config.dataCopyBatchSize))
    logSearchRequest(scrollInitRequest, types)
    val scrollInit = scrollInitRequest.execute.actionGet

    var scrollId = scrollInit.getScrollId
    var batch = Seq.empty[Any]
    val resultCounts = mutable.Map[String, Int]()

    do {
      val request = client.prepareSearchScroll(scrollId)
        .setScroll(timeout)
      logSearchScrollRequest(scrollId, timeout.toString)
      val response = request.execute.actionGet
      logSearchResponse(response)

      scrollId = response.getScrollId
      batch = response.getHits.hits.map { h =>
        client.prepareDelete(h.index, h.`type`, h.id)
      }

      sendBulkRequest(batch, refresh = false).foreach { response =>
        resultCounts ++= response.deletions.map { case (k, v) => (k, resultCounts.getOrElse(k, 0) + v) }
      }
    } while (batch.nonEmpty)

    if (refresh) this.refresh()

    resultCounts.toMap
  }

  def copyFieldValues(from: DatasetCopy, to: DatasetCopy, refresh: Boolean): Unit = {
    logCopyFieldValuesRequest(from, to, refresh)
    val timeout = new TimeValue(config.dataCopyTimeout)
    val scrollInitRequest = client.prepareSearch(config.index)
      .setTypes(config.fieldValueMapping.mappingType)
      .setQuery(byCopyNumberQuery(from.datasetId, from.copyNumber))
      .setSearchType(SearchType.SCAN)
      .setScroll(timeout)
      .setSize(calculateScrollSize(config.dataCopyBatchSize))
    logSearchRequest(scrollInitRequest, Seq(config.fieldValueMapping.mappingType))
    val scrollInit = scrollInitRequest.execute.actionGet

    var scrollId = scrollInit.getScrollId
    var batch = Seq.empty[Any]
    do {
      val request = client.prepareSearchScroll(scrollId)
        .setScroll(timeout)
      logSearchScrollRequest(scrollId, timeout.toString)
      val response = request.execute.actionGet
      logSearchResponse(response)

      scrollId = response.getScrollId
      batch = response.results[FieldValue].thisPage.map { src =>
        fieldValueIndexRequest(FieldValue(src.datasetId, to.copyNumber, src.columnId, src.rowId, src.value))
      }

      if (batch.nonEmpty) {
        sendBulkRequest(batch, refresh = false)
      }
    } while (batch.nonEmpty)

    // TODO : Guarantee refresh before read instead of after write
    if (refresh) {
      this.refresh()
    }
  }

  def deleteFieldValuesByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(config.fieldValueMapping.mappingType))

  def deleteFieldValuesByCopyNumber(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(config.fieldValueMapping.mappingType))

  def deleteFieldValuesByRowId(datasetId: String, copyNumber: Long, rowId: Long): Unit =
    deleteByQuery(byRowIdQuery(datasetId, copyNumber, rowId), Seq(config.fieldValueMapping.mappingType))

  def deleteFieldValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): Unit =
    deleteByQuery(byColumnIdQuery(datasetId, copyNumber, columnId), Seq(config.fieldValueMapping.mappingType))

  def putDatasetCopy(datasetId: String,
                     copyNumber: Long,
                     dataVersion: Long,
                     stage: LifecycleStage,
                     refresh: Boolean): Unit = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    val request = client.prepareIndex(config.index, config.datasetCopyMapping.mappingType, id)
      .setSource(source)
      .setRefresh(refresh)
    logDatasetCopyIndexRequest(id, source)
    request.execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy, refresh: Boolean): Unit = {
    val source = JsonUtil.renderJson(datasetCopy)
    val request = client.prepareUpdate(config.index, config.datasetCopyMapping.mappingType, datasetCopy.docId)
      .setDoc(source)
      .setUpsert()
      .setRefresh(refresh)
    logDatasetCopyUpdateRequest(datasetCopy, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  private def datasetIdAndOptionalStageQuery(datasetId: String, stage: Option[Stage]): QueryBuilder =
    stage match {
      case Some(n @ Number(_)) => throw new IllegalArgumentException(s"cannot request latest copy for stage = $n")
      case Some(Unpublished) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Unpublished)
      case Some(Published) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Published)
      case Some(Snapshotted) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Snapshotted)
      case Some(Discarded) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Discarded)
      case _ => byDatasetIdQuery(datasetId)
    }

  def datasetCopyLatest(datasetId: String, stage: Option[Stage] = None): Option[DatasetCopy] = {
    val latestCopyPlaceholder = "latest_copy"
    val query = datasetIdAndOptionalStageQuery(datasetId, stage)

    val request = client.prepareSearch(config.index)
      .setTypes(config.datasetCopyMapping.mappingType)
      .setQuery(query)
      .setSize(1)
      .addSort(SpandexFields.CopyNumber, SortOrder.DESC)
      .addAggregation(max(latestCopyPlaceholder).field(SpandexFields.CopyNumber))
    logDatasetCopySearchRequest(request)

    val response = request.execute.actionGet
    val results = response.results[DatasetCopy]
    logDatasetCopySearchResults(results)
    results.thisPage.headOption
  }

  def datasetCopiesByStage(datasetId: String, stage: Stage): List[DatasetCopy] = {
    val query = datasetIdAndOptionalStageQuery(datasetId, Some(stage))

    val countRequest = client.prepareCount(config.index)
      .setTypes(config.datasetCopyMapping.mappingType)
      .setQuery(query)

    val count = countRequest.execute.actionGet.result

    val request = client.prepareSearch(config.index)
      .setTypes(config.datasetCopyMapping.mappingType)
      .setQuery(query)
      .setSize(count.toInt)

    val response = request.execute.actionGet
    val datasetCopies = response.results[DatasetCopy]

    datasetCopies.thisPage.toList
  }

  def datasetCopy(datasetId: String, copyNumber: Long): Option[DatasetCopy] = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val request = client.prepareGet(config.index, config.datasetCopyMapping.mappingType, id)
    logDatasetCopyGetRequest(id)
    val response = request.execute.actionGet
    val datasetCopy = response.result[DatasetCopy]
    logDatasetCopyGetResult(datasetCopy)
    datasetCopy
  }

  def deleteDatasetCopy(datasetId: String, copyNumber: Long): Unit =
    deleteByQuery(byCopyNumberQuery(datasetId, copyNumber), Seq(config.datasetCopyMapping.mappingType))

  def deleteDatasetCopiesByDataset(datasetId: String): Unit =
    deleteByQuery(byDatasetIdQuery(datasetId), Seq(config.datasetCopyMapping.mappingType))

  private val allTypes = Seq(
    config.datasetCopyMapping.mappingType,
    config.columnMapMapping.mappingType,
    config.fieldValueMapping.mappingType)

  def deleteDatasetById(datasetId: String): Map[String, Int] =
    deleteByQuery(byDatasetIdQuery(datasetId), allTypes)

  def suggest(column: ColumnMap, size: Int, text: String,
              fuzz: Fuzziness, fuzzLength: Int, fuzzPrefix: Int): Suggest = {
    val suggestion = new CompletionSuggestionFuzzyBuilder("suggest")
      .addContextField(SpandexFields.CompositeId, column.compositeId)
      .setFuzziness(fuzz).setFuzzyPrefixLength(fuzzPrefix).setFuzzyMinLength(fuzzLength)
      .field(SpandexFields.Value)
      .text(text)
      .size(size)

    // if you ever need to examine the above, you want to do something like:
    // suggestion.toXContent(JsonXContent.contentBuilder(), ToXContent.EMPTY_PARAMS).prettyPrint().string()

    val request = client.prepareSuggest(config.index)
      .addSuggestion(suggestion)

    val response = request.execute.actionGet

    response.getSuggest
  }
}
