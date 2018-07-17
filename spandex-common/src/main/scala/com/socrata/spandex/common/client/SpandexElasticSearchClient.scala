package com.socrata.spandex.common.client

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.io.Source
import java.io.Closeable
import java.net.InetAddress

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary._
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.{DeleteRequestBuilder, DeleteResponse}
import org.elasticsearch.action.index.{IndexRequestBuilder, IndexResponse}
import org.elasticsearch.action.update.{UpdateRequestBuilder, UpdateResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.rest.RestStatus.{CREATED, OK}
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.aggregations.AggregationBuilders.max
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import org.elasticsearch.transport.client.PreBuiltTransportClient

import com.socrata.spandex.common.{ElasticSearchConfig, ElasticsearchClientLogger}
import com.socrata.spandex.common.client.ResponseExtensions._
import com.socrata.spandex.common.client.Queries._
import SpandexElasticSearchClient.{DatasetCopyType, ColumnType, ColumnValueType}

// scalastyle:off number.of.methods
case class ElasticSearchResponseFailed(msg: String) extends Exception(msg)

class SpandexElasticSearchClient(
    val client: Client,
    val indexName: String,
    val dataCopyBatchSize: Int,
    val dataCopyTimeout: Long,
    val maxColumnValueLength: Int)
  extends Closeable
  with ElasticsearchClientLogger {

  def this(
    host: String,
    port: Int,
    clusterName: String,
    indexName: String,
    dataCopyBatchSize: Int,
    dataCopyTimeout: Long,
    maxColumnValueLength: Int
  ) = {
    this(
      new PreBuiltTransportClient(
        Settings.builder().put("cluster.name", clusterName).put("client.transport.sniff", true).build()
      ).addTransportAddress(
        new InetSocketTransportAddress(InetAddress.getByName(host), port)
      ),
      indexName,
      dataCopyBatchSize,
      dataCopyTimeout,
      maxColumnValueLength
    )
  }

  def this(config: ElasticSearchConfig) = {
    this(
      config.host,
      config.port,
      config.clusterName,
      config.index,
      config.dataCopyBatchSize,
      config.dataCopyTimeout,
      config.maxColumnValueLength
    )
  }

  logClientConnected()
  logClientHealthCheckStatus()

  override def close(): Unit = client.close()

  private[this] def checkForFailures(response: ActionResponse): Unit = response match {
    case i: IndexResponse =>
      if (!Set(OK, CREATED).contains(i.status())) throw ElasticSearchResponseFailed(
        s"${i.getType} doc with id ${i.getId} was not successfully indexed; received status ${i.status()}")
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

  def refresh(): Unit =
    client.admin().indices().prepareRefresh(indexName).execute.actionGet

  def indexExists: Boolean = {
    logIndexExistsRequest(indexName)
    val request = client.admin.indices.exists(new IndicesExistsRequest(indexName))
    val result = request.actionGet.isExists
    logIndexExistsResult(indexName, result)
    result
  }

  def putColumnMap(columnMap: ColumnMap, refresh: RefreshPolicy = Eventually): Unit = {
    val source = JsonUtil.renderJson(columnMap)
    val request = client.prepareIndex(indexName, ColumnType, columnMap.docId)
      .setSource(source, XContentType.JSON)
      .setRefreshPolicy(refresh)
    logColumnMapIndexRequest(columnMap.docId, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  def fetchColumnMap(datasetId: String, copyNumber: Long, userColumnId: String): Option[ColumnMap] = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val response = client.prepareGet(indexName, ColumnType, id).execute.actionGet
    response.result[ColumnMap]
  }

  def deleteColumnMap(
      datasetId: String,
      copyNumber: Long,
      userColumnId: String,
      refresh: RefreshPolicy = Eventually)
    : Unit = {
    val id = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
    val delete = client.prepareDelete(indexName, ColumnType, id)
    delete.setRefreshPolicy(refresh)

    checkForFailures(delete.execute.actionGet)
  }

  def deleteColumnMapsByDataset(datasetId: String, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetId(datasetId), Seq(ColumnType), refresh)

  // We don't expect the number of column maps to exceed dataCopyBatchSize.
  // As of April 2015 the widest dataset is ~1000 cols wide.
  def searchLotsOfColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] =
    client.prepareSearch(indexName)
      .setTypes(ColumnType)
      .setQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber))
      .setSize(dataCopyBatchSize)
      .execute.actionGet.results[ColumnMap]()

  def deleteColumnMapsByCopyNumber(datasetId: String, copyNumber: Long, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber), Seq(ColumnType), refresh)

  def columnValueUpsertRequest(columnValue: ColumnValue): UpdateRequestBuilder = {
    val script = new Script(
      ScriptType.INLINE,
      "painless",
      "ctx._source.count += params.count",
      Map("count" -> columnValue.count.asInstanceOf[Object]).asJava)

    client.prepareUpdate(indexName, ColumnValueType, columnValue.docId)
      .setScript(script)
      .setUpsert(JsonUtil.renderJson(columnValue.truncate(maxColumnValueLength)), XContentType.JSON)
  }

  def indexColumnValues(columnValues: Iterable[ColumnValue], refresh: RefreshPolicy = Eventually): Unit =
    sendBulkRequest(
      columnValues.collect {
        case cv: ColumnValue if cv.isNonEmpty =>
          columnValueUpsertRequest(cv)
      }, refresh
    )

  def putColumnValues(
      datasetId: String,
      copyNumber: Long,
      columnValues: Iterable[ColumnValue],
      refresh: RefreshPolicy = Eventually)
    : Unit = {
    columnValues.grouped(dataCopyBatchSize).foreach { batch =>
      indexColumnValues(batch, Eventually)
    }

    if (refresh != Eventually) this.refresh()
  }

  def columnValueIndexRequest(columnValue: ColumnValue): IndexRequestBuilder =
    client.prepareIndex(indexName, ColumnValueType, columnValue.docId)
      .setSource(JsonUtil.renderJson(columnValue.truncate(maxColumnValueLength)), XContentType.JSON)

  def copyColumnValues(from: DatasetCopy, to: DatasetCopy, refresh: RefreshPolicy = Eventually): Unit = {
    logCopyColumnValuesRequest(from, to, refresh)
    val timeout = new TimeValue(dataCopyTimeout)
    val request = client.prepareSearch(indexName)
      .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
      .setTypes(ColumnValueType)
      .setQuery(byDatasetIdAndCopyNumber(from.datasetId, from.copyNumber))
      .setScroll(timeout)
      .setSize(calculateScrollSize(dataCopyBatchSize))

    logSearchRequest(request, Seq(ColumnValueType))
    var response = request.execute.actionGet

    do {
      val batch = response.results[ColumnValue]().thisPage.map { case ScoredResult(src, _) =>
        columnValueIndexRequest(ColumnValue(src.datasetId, to.copyNumber, src.columnId, src.value, src.count))
      }

      if (batch.nonEmpty) {
        sendBulkRequest(batch, refresh = Eventually)
      }
      response = client.prepareSearchScroll(response.getScrollId)
        .setScroll(timeout).execute.actionGet
      logSearchResponse(response)
    } while (response.getHits().getHits().length != 0)

    if (refresh != Eventually) this.refresh()
  }

  def deleteColumnValuesByDataset(datasetId: String, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetId(datasetId), Seq(ColumnValueType), refresh)

  def deleteColumnValuesByCopyNumber(datasetId: String, copyNumber: Long, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber), Seq(ColumnValueType), refresh)

  def deleteNonPositiveCountColumnValues(
      datasetId: String,
      copyNumber: Long,
      refresh: RefreshPolicy = Eventually)
    : Unit =
    deleteByQuery(
      nonPositiveCountColumnValuesByDatasetIdAndCopyNumber(datasetId, copyNumber),
      Seq(ColumnValueType),
      refresh)

  def deleteColumnValuesByColumnId(
      datasetId: String,
      copyNumber: Long,
      columnId: Long,
      refresh: RefreshPolicy = Eventually)
    : Unit =
    deleteByQuery(byDatasetIdCopyNumberAndColumnId(datasetId, copyNumber, columnId), Seq(ColumnValueType), refresh)

  // Yuk @ Seq[Any], but the number of types on ActionRequestBuilder is absurd.
  def sendBulkRequest(requests: Iterable[Any], refresh: RefreshPolicy = Eventually): Option[BulkResponse] = {
    if (requests.nonEmpty) {
      val baseRequest = client.prepareBulk().setRefreshPolicy(refresh)
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
    * Note: the value `indexName` could be an alias, and it's possible to iterate over returned settings and find
    * a concrete index with corresponding alias. But *index* vs *total* primary shards is almost the same number.
    */
  private def calculateScrollSize(desiredSize: Int): Int = {
    val clusterHealthRequest = client.admin.cluster.prepareHealth()
    val clusterHealthResponse = clusterHealthRequest.execute.actionGet
    val primaryShardCount = clusterHealthResponse.getActivePrimaryShards
    math.max(1, desiredSize / primaryShardCount)
  }

  def deleteByQuery(
      queryBuilder: QueryBuilder,
      types: Seq[String] = Nil,
      refresh: RefreshPolicy = Eventually)
    : Map[String, Int] = {

    val timeout = new TimeValue(dataCopyTimeout)
    val request = client.prepareSearch(indexName)
      .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
      .setQuery(queryBuilder)
      .setTypes(types: _*)
      .setScroll(timeout)
      .setSize(calculateScrollSize(dataCopyBatchSize))

    logDeleteByQueryRequest(queryBuilder, types, refresh)
    var response = request.execute.actionGet
    val resultCounts = mutable.Map[String, Int]()

    do {
      val batch = response.getHits.getHits.map { h =>
        client.prepareDelete(h.getIndex, h.getType, h.getId)
      }

      sendBulkRequest(batch, refresh = Eventually).foreach { response =>
        resultCounts ++= response.deletions.map { case (k, v) => (k, resultCounts.getOrElse(k, 0) + v) }
      }

      response = client.prepareSearchScroll(response.getScrollId)
        .setScroll(timeout).execute.actionGet
      logSearchResponse(response)
    } while (response.getHits().getHits().length != 0)

    if (refresh != Eventually) this.refresh()

    resultCounts.toMap
  }

  def putDatasetCopy(
      datasetId: String,
      copyNumber: Long,
      dataVersion: Long,
      stage: LifecycleStage,
      refresh: RefreshPolicy = Eventually)
    : Unit = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val source = JsonUtil.renderJson(DatasetCopy(datasetId, copyNumber, dataVersion, stage))
    val request = client.prepareIndex(indexName, DatasetCopyType, id)
      .setSource(source, XContentType.JSON)
      .setRefreshPolicy(refresh)
    logDatasetCopyIndexRequest(id, source)
    request.execute.actionGet
  }

  def updateDatasetCopyVersion(datasetCopy: DatasetCopy, refresh: RefreshPolicy = Eventually): Unit = {
    val source = JsonUtil.renderJson(datasetCopy)
    val request = client.prepareUpdate(indexName, DatasetCopyType, datasetCopy.docId)
      .setDoc(source, XContentType.JSON)
      .setUpsert()
      .setRefreshPolicy(refresh)
    logDatasetCopyUpdateRequest(datasetCopy, source)
    val response = request.execute.actionGet
    checkForFailures(response)
  }

  def datasetCopyLatest(datasetId: String, stage: Option[Stage] = None): Option[DatasetCopy] = {
    val latestCopyPlaceholder = "latest_copy"
    val query = byDatasetIdAndOptionalStage(datasetId, stage)

    val request = client.prepareSearch(indexName)
      .setTypes(DatasetCopyType)
      .setQuery(query)
      .setSize(1)
      .addSort(SpandexFields.CopyNumber, SortOrder.DESC)
      .addAggregation(max(latestCopyPlaceholder).field(SpandexFields.CopyNumber))
    logDatasetCopySearchRequest(request)

    val response = request.execute.actionGet
    val results = response.results[DatasetCopy]()
    logDatasetCopySearchResults(results)
    results.thisPage.headOption.map(_.result)
  }

  def datasetCopiesByStage(datasetId: String, stage: Stage): List[DatasetCopy] = {
    val query = byDatasetIdAndOptionalStage(datasetId, Some(stage))

    val countRequest = client.prepareSearch(indexName)
      .setSize(0)
      .setTypes(DatasetCopyType)
      .setQuery(query)

    val count = countRequest.execute.actionGet

    val request = client.prepareSearch(indexName)
      .setTypes(DatasetCopyType)
      .setQuery(query)
      .setSize(count.getHits.totalHits.toInt)

    val response = request.execute.actionGet
    val datasetCopies = response.results[DatasetCopy]()

    datasetCopies.thisPage.map(_.result).toList
  }

  def datasetCopy(datasetId: String, copyNumber: Long): Option[DatasetCopy] = {
    val id = DatasetCopy.makeDocId(datasetId, copyNumber)
    val request = client.prepareGet(indexName, DatasetCopyType, id)
    logDatasetCopyGetRequest(id)
    val response = request.execute.actionGet
    val datasetCopy = response.result[DatasetCopy]
    logDatasetCopyGetResult(datasetCopy)
    datasetCopy
  }

  def deleteDatasetCopy(datasetId: String, copyNumber: Long, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber), Seq(DatasetCopyType), refresh)

  def deleteDatasetCopiesByDataset(datasetId: String, refresh: RefreshPolicy = Eventually): Unit =
    deleteByQuery(byDatasetId(datasetId), Seq(DatasetCopyType), refresh)

  private val allTypes = Seq(DatasetCopyType, ColumnType, ColumnValueType)

  def deleteDatasetById(datasetId: String, refresh: RefreshPolicy = Eventually): Map[String, Int] =
    deleteByQuery(byDatasetId(datasetId), allTypes, refresh)

  def suggest(column: ColumnMap, size: Int, text: String): SearchResults[ColumnValue] = {
    val suggestText = if (text.length > 0) Some(text) else None

    val query = byColumnValueAutocompleteAndCompositeId(suggestText, column)

    def baseRequest =
      client.prepareSearch(indexName)
        .setSize(size)
        .setTypes(ColumnValueType)
        .setQuery(query)

    // NOTE: When no text is provided, we include in the request a sort parameter (using the column
    // value count field). When text is provided, we simply use Elasticsearch's default scoring.
    val (request, scoringFn) = suggestText match {
      case None =>
        val scoringFn = (cv: ColumnValue) => cv.count.toFloat
        (baseRequest.addSort(SpandexFields.Count, SortOrder.DESC), Some(scoringFn))
      case _ => (baseRequest, None)
    }

    request.execute.actionGet.results[ColumnValue](scoringFn)
  }
}

object SpandexElasticSearchClient {
  val DatasetCopyType = "dataset_copy"
  val ColumnType = "column"
  val ColumnValueType = "column_value"

  def readSettings(): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"settings.json")
    Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def readMapping(esType: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"mapping.$esType.json")
    Source.fromInputStream(stream).getLines().mkString("\n")
  }

  def ensureIndex(index: String, esClient: SpandexElasticSearchClient): Unit = {
    if (!esClient.indexExists) {
       try {
         esClient.logIndexCreateRequest(index)

         esClient.client.admin.indices.prepareCreate(index)
           .setSettings(readSettings, XContentType.JSON)
           .execute.actionGet

         // Add column_value mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(ColumnValueType)
           .setSource(readMapping(ColumnValueType), XContentType.JSON).execute.actionGet

         // Add column_map mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(ColumnType)
           .setSource(readMapping(ColumnType), XContentType.JSON).execute.actionGet

         // Add dataset_copy mapping
         esClient.client.admin.indices.preparePutMapping(index)
           .setType(DatasetCopyType)
           .setSource(readMapping(DatasetCopyType), XContentType.JSON).execute.actionGet
       } catch {
         // TODO: more error handling
         case e: ElasticsearchException =>
           esClient.logIndexAlreadyExists(index)
       }
    }
  }
}
