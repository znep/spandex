package com.socrata.spandex.common.client

import org.elasticsearch.client.{Requests, Client}
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.index.query.QueryBuilders._
import com.socrata.spandex.common.{SpandexBootstrap, ElasticSearchConfig}

class TestESClient(config: ElasticSearchConfig) extends SpandexElasticSearchClient(config) {
  val node = nodeBuilder().local(true).node()

  override val client: Client = node.client()

  SpandexBootstrap.ensureIndex(config, this)

  def deleteIndex(): Unit = {
    client.admin().indices().delete(Requests.deleteIndexRequest(config.index))
  }

  def deleteAllDatasetCopies(): Unit = {
    client.prepareDeleteByQuery(config.index)
          .setQuery(termQuery("_type", config.datasetCopyMapping))
          .execute().actionGet()
  }

  override def close(): Unit = {
    deleteIndex()
    node.close()
    super.close()
  }
}
