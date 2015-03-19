package com.socrata.spandex.common

import com.socrata.spandex.common.client.ElasticSearchClient
import org.elasticsearch.client.{Requests, Client}
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.indices.IndexAlreadyExistsException

class TestESClient(config: ElasticSearchConfig)
  extends ElasticSearchClient("local", 1234, config.clusterName, config.index, config.mappingType) {

  val node = nodeBuilder().local(true).node()

  override val client: Client = node.client()

  // Bootstrap Spandex index and mappings
  try {
    client.admin().indices().create(Requests.createIndexRequest(config.index)).actionGet
    client.admin().indices().preparePutMapping(config.index)
      .setType(config.mappingType)
      .setSource(config.mappingProperties).execute.actionGet
  } catch {
    case exists: IndexAlreadyExistsException => // if the index already exists, we're good
  }

  override def close(): Unit = {
    client.admin().indices().delete(Requests.deleteIndexRequest(config.index))
    node.close()
    client.close()
  }
}