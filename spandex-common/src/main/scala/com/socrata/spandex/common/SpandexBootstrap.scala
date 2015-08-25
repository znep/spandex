package com.socrata.spandex.common

import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.indices.IndexAlreadyExistsException

object SpandexBootstrap extends Logging {
  def ensureIndex(config: ElasticSearchConfig, esClient: SpandexElasticSearchClient): Unit = {
    if (!esClient.indexExists) {
       try {
         logger.info("creating index {} on {}", config.index, config.clusterName)
         esClient.client.admin().indices().prepareCreate(config.index)
           .setSettings(config.settings)
           .execute.actionGet

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
       } catch {
         // TODO: more error handling
         case e: IndexAlreadyExistsException =>
           logger.info("actually that index ({}) already exists on cluster ({})", config.index, config.clusterName)
       }
    }
  }
}
