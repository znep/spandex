package com.socrata.spandex.common

import org.elasticsearch.ElasticsearchException
import org.elasticsearch.common.xcontent.XContentType

import com.socrata.spandex.common.client.SpandexElasticSearchClient

object SpandexBootstrap extends ElasticsearchClientLogger {
  def ensureIndex(config: ElasticSearchConfig, esClient: SpandexElasticSearchClient): Unit = {
    if (!esClient.indexExists) {
       try {
         logIndexCreateRequest(config.index, config.clusterName)
         esClient.client.admin.indices.prepareCreate(config.index)
           .setSettings(config.settings, XContentType.JSON)
           .execute.actionGet

         // Add field_value mapping
         esClient.client.admin.indices.preparePutMapping(config.index)
           .setType(config.fieldValueMapping.mappingType)
           .setSource(config.fieldValueMapping.mappingProperties, XContentType.JSON).execute.actionGet

         // Add column_map mapping
         esClient.client.admin.indices.preparePutMapping(config.index)
           .setType(config.columnMapMapping.mappingType)
           .setSource(config.columnMapMapping.mappingProperties, XContentType.JSON).execute.actionGet

         // Add dataset_copy mapping
         esClient.client.admin.indices.preparePutMapping(config.index)
           .setType(config.datasetCopyMapping.mappingType)
           .setSource(config.datasetCopyMapping.mappingProperties, XContentType.JSON).execute.actionGet
       } catch {
         // TODO: more error handling
         case e: ElasticsearchException =>
           logIndexAlreadyExists(config.index, config.clusterName)
       }
    }
  }
}
