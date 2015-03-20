package com.socrata.spandex.common.client

import org.elasticsearch.client.{Requests, Client}
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.elasticsearch.index.query.QueryBuilders._
import com.socrata.spandex.common.ElasticSearchConfig

class TestESClient(config: ElasticSearchConfig) extends SpandexElasticSearchClient(config) {

  val node = nodeBuilder().local(true).node()

  override val client: Client = node.client()

  // Bootstrap Spandex index and mappings
  try {
    client.admin().indices().create(Requests.createIndexRequest(config.index)).actionGet
    client.admin().indices().preparePutMapping(config.index)
      .setType(config.fieldValueMapping.mappingType)
      .setSource(config.fieldValueMapping.mappingProperties).execute.actionGet
    client.admin().indices().preparePutMapping(config.index)
      .setType(config.datasetCopyMapping.mappingType)
      .setSource(config.datasetCopyMapping.mappingProperties).execute.actionGet
  } catch {
    case exists: IndexAlreadyExistsException => // if the index already exists, we're good
  }

  def deleteAllDatasetCopies(): Unit = {
    val response = client.prepareDeleteByQuery(config.index)
                         .setQuery(termQuery("_type", config.datasetCopyMapping))
                         .execute().actionGet()
  }

  override def close(): Unit = {
    client.admin().indices().delete(Requests.deleteIndexRequest(config.index))
    node.close()
    client.close()
  }
}
