package com.socrata.spandex.common

import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.client.Requests

object SpandexBootstrap extends Logging {
  def ensureIndex(config: ElasticSearchConfig, esClient: SpandexElasticSearchClient): Unit = {
    if (!esClient.indexExists) {
      logger.info("creating index {} on {}", config.index, config.clusterName)
      esClient.client.admin().indices().create(Requests.createIndexRequest(config.index)).actionGet

      // Add field_value mapping
      esClient.client.admin().indices().preparePutMapping(config.index)
        .setType(config.fieldValueMapping.mappingType)
        .setSource(config.fieldValueMapping.mappingProperties).execute.actionGet

      // Add column_map mapping
      esClient.client.admin().indices().preparePutMapping(config.index)
        .setType(config.columnMapMapping.mappingType)
        .setSource(config.columnMapMapping.mappingProperties).execute.actionGet

      // Add dataset_copy mapping
      esClient.client.admin().indices().preparePutMapping(config.index)
        .setType(config.datasetCopyMapping.mappingType)
        .setSource(config.datasetCopyMapping.mappingProperties).execute.actionGet
    }
    // TODO: error handling
  }
}
