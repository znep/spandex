package com.socrata.spandex.common.client

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal
import java.net.InetAddress
import java.io.Closeable

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders.{boolQuery, termQuery}
import org.elasticsearch.transport.client.PreBuiltTransportClient

import com.socrata.spandex.common.{TestData, Token}
import Queries._
import ResponseExtensions._
import SpandexElasticSearchClient.{ColumnType, ColumnValueType, DatasetCopyType}

class SpandexESIntegrationTestClient(val client: SpandexElasticSearchClient) extends Closeable {
  def ensureIndex(): Unit =
    SpandexElasticSearchClient.ensureIndex(client.indexName, client)

  private val healthTimeoutMillis = 2000
  private val acceptableClusterHealthStatuses = List(ClusterHealthStatus.GREEN, ClusterHealthStatus.YELLOW)

  override def close(): Unit = client.close()

  def isConnected: Boolean = try {
    val status = client.client.admin().cluster().prepareHealth()
      .execute.actionGet(healthTimeoutMillis).getStatus
    acceptableClusterHealthStatuses.contains(status)
  } catch {
    case NonFatal(e) =>
      false
  }

  def bootstrapData(testData: TestData): Unit = {
    // Create 2 datasets with 3 copies each:
    // - an old snapshot copy
    // - the most recent published copy
    // - a working copy
    testData.datasets.foreach { ds =>
      testData.copies(ds).foreach { copy =>
        client.putDatasetCopy(ds, copy.copyNumber, copy.version, copy.stage, refresh = Immediately)

        testData.columns(ds, copy).foreach { col =>
          client.putColumnMap(
            ColumnMap(ds, copy.copyNumber, col.systemColumnId, col.userColumnId),
            refresh = Immediately
          )

          testData.rows(col).foreach(indexColumnValue)
        }
      }
    }
  }

  def removeBootstrapData(testData: TestData): Unit = {
    testData.datasets.foreach { d =>
      client.deleteColumnValuesByDataset(d)
      client.deleteColumnMapsByDataset(d)
      client.deleteDatasetCopiesByDataset(d)
    }

    client.refresh()
  }

  def deleteIndex(): Unit =
    client.client.admin().indices().delete(new DeleteIndexRequest(client.indexName)).actionGet()
  
  def deleteAllDatasetCopies(): Unit =
    client.deleteByQuery(termQuery("_type", DatasetCopyType), refresh = Immediately)

  def searchColumnValuesByCopyNumber(datasetId: String, copyNumber: Long, size: Int = 10): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber))
      .setSize(size)
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnValuesByColumnId(datasetId: String, copyNumber: Long, columnId: Long): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byDatasetIdCopyNumberAndColumnId(datasetId, copyNumber, columnId))
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnMapsByCopyNumber(datasetId: String, copyNumber: Long): SearchResults[ColumnMap] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnType)
      .setQuery(byDatasetIdAndCopyNumber(datasetId, copyNumber))
      .execute.actionGet
    response.results[ColumnMap]()
  }

  def indexColumnValue(columnValue: ColumnValue): Unit =
    client.indexColumnValues(Seq(columnValue), refresh = Immediately)

  def fetchColumnValue(columnValue: ColumnValue): Option[ColumnValue] = {
    val response = client.client.prepareGet(client.indexName, ColumnValueType, columnValue.docId)
      .execute.actionGet
    response.result[ColumnValue]
  }

  def searchColumnValuesByDataset(datasetId: String): SearchResults[ColumnValue] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(byDatasetId(datasetId))
      .execute.actionGet
    response.results[ColumnValue]()
  }

  def searchColumnMapsByDataset(datasetId: String): SearchResults[ColumnMap] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnType)
      .setQuery(byDatasetId(datasetId))
      .execute.actionGet
    response.results[ColumnMap]()
  }

  def searchCopiesByDataset(datasetId: String): SearchResults[DatasetCopy] = {
    val response = client.client.prepareSearch(client.indexName)
      .setTypes(DatasetCopyType)
      .setQuery(byDatasetId(datasetId))
      .execute.actionGet
    response.results[DatasetCopy]()
  }

  def fetchCountForColumnValue(datasetId: String, value: String): Option[Long] = {
    val query = boolQuery().must(byDatasetId(datasetId)).must(termQuery("value", value))

    val response = client.client.prepareSearch(client.indexName)
      .setTypes(ColumnValueType)
      .setQuery(query)
      .execute.actionGet

    response.results[ColumnValue]().thisPage.map(_.result.count).headOption
  }

  def analyze(analyzer: String, text: String): List[Token] = {
    val request = new AnalyzeRequest(client.indexName).analyzer(analyzer).text(text)
    client.client.admin().indices().analyze(request).actionGet().getTokens.asScala.map(
      Token.apply).toList
  }
}

object SpandexESIntegrationTestClient {
  def apply(
      host: String,
      port: Int,
      clusterName: String,
      indexName: String,
      dataCopyBatchSize: Int,
      dataCopyTimeout: Long,
      maxColumnValueLength: Int)
    : SpandexESIntegrationTestClient = {

    val settings = Settings.builder()
      .put("cluster.name", clusterName)
      .build()

    val transportAddress = new InetSocketTransportAddress(InetAddress.getByName(host), port)
    val transportClient: Client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress)

    val client = new SpandexElasticSearchClient(
      transportClient, indexName, dataCopyBatchSize, dataCopyTimeout, maxColumnValueLength)

    new SpandexESIntegrationTestClient(client)
  }

  implicit def underlying(client: SpandexESIntegrationTestClient): SpandexElasticSearchClient =
    client.client
}
