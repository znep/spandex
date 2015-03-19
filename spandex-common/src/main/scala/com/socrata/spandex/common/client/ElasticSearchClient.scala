package com.socrata.spandex.common.client

import java.io.Closeable

import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{Settings, ImmutableSettings}
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.QueryBuilder
import com.socrata.spandex.common.ElasticSearchConfig

class ElasticSearchClient(host: String,
                          port: Int,
                          clusterName: String,
                          index: String,
                          mappingType: String) extends Closeable {
  def this(config: ElasticSearchConfig) =
    this(config.host, config.port, config.clusterName, config.mappingType, config.mappingProperties)

  val settings: Settings = ImmutableSettings.settingsBuilder()
                                            .put("cluster.name", clusterName)
                                            .put("client.transport.sniff", true)
                                            .put("node.name", "Wendigo") // TODO : don't hardcode the node name
                                            .build()

  val transportAddress = new InetSocketTransportAddress(host, port)
  val client: Client = new TransportClient(settings).addTransportAddress(transportAddress)

  private def byDatasetQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.datasetId, datasetId)
  private def byCopyIdQuery(datasetId: String, copyId: Long): QueryBuilder =
    boolQuery().must(termQuery(SpandexFields.datasetId, datasetId)).must(termQuery(SpandexFields.copyId, copyId))

  def close(): Unit = client.close()

  def searchByDataset(datasetId: String): SearchResponse =
    client.prepareSearch()
          .setTypes(mappingType)
          .setQuery(byDatasetQuery(datasetId))
          .execute.actionGet

  def deleteByDataset(datasetId: String): DeleteByQueryResponse =
    client.prepareDeleteByQuery(index)
          .setTypes(mappingType)
          .setQuery(byDatasetQuery(datasetId))
          .execute.actionGet

  def searchByCopyId(datasetId: String, copyId: Long): SearchResponse =
    client.prepareSearch()
          .setTypes(mappingType)
          .setQuery(byCopyIdQuery(datasetId, copyId))
          .execute.actionGet

  def deleteByCopyId(datasetId: String, copyId: Long): DeleteByQueryResponse =
    client.prepareDeleteByQuery(index)
          .setTypes(mappingType)
          .setQuery(byCopyIdQuery(datasetId, copyId))
          .execute.actionGet
}
