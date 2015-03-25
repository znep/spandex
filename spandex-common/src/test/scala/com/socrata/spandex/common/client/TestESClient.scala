package com.socrata.spandex.common.client

import java.nio.file.Files

import com.socrata.spandex.common.{ElasticSearchConfig, SpandexBootstrap}
import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.node.NodeBuilder._

class TestESClient(config: ElasticSearchConfig) extends SpandexElasticSearchClient(config) {
  private[this] val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  private[this] val settings = ImmutableSettings.settingsBuilder
    .put("path.data", dataDir.toString)
    .build

  val node = nodeBuilder().settings(settings).local(true).node()

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
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception => // dataDir cleanup failed
    }

    super.close()
  }
}
