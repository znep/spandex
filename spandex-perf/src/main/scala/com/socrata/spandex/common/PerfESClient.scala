package com.socrata.spandex.common

import java.nio.file.Files

import com.socrata.spandex.common.client.SpandexElasticSearchClient
import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._

class PerfESClient(config: SpandexConfig = new SpandexConfig) extends SpandexElasticSearchClient(config.es) {
  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val local: Boolean = true
  val testSettings = ImmutableSettings.settingsBuilder()
    .put(settings)
    .put("path.data", tempDataDir.toString)
    .build
  val node = nodeBuilder().settings(testSettings).local(local).node()
  override val client: Client = node.client()

  def bootstrapData(): Unit = {
    SpandexBootstrap.ensureIndex(config.es, this)
  }

  def removeBootstrapData(): Unit = {
    deleteIndex()
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

  def deleteIndex(): Unit = client.admin().indices().delete(Requests.deleteIndexRequest(config.es.index))
}
