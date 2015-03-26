package com.socrata.spandex.common.client

import java.nio.file.Files

import com.socrata.spandex.common.{ElasticSearchConfig, SpandexBootstrap}
import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.node.NodeBuilder._

class TestESClient(config: ElasticSearchConfig, local: Boolean = true) extends SpandexElasticSearchClient(config) {
  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = ImmutableSettings.settingsBuilder()
    .put(settings)
    .put("path.data", tempDataDir.toString)
    .build

  val node = nodeBuilder().settings(testSettings).local(local).node()

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

    try {
      FileUtils.forceDelete(tempDataDir)
    } catch {
      case e: Exception => // cleanup failed
    }

    super.close()
  }
}
