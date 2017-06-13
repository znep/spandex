package com.socrata.spandex.common

import java.nio.file.Files
import scala.util.Try

import org.apache.commons.io.FileUtils
import org.elasticsearch.client.{Client, Requests}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.Node

import com.socrata.spandex.common.client.SpandexElasticSearchClient

class PerfESClient(config: SpandexConfig = new SpandexConfig) extends SpandexElasticSearchClient(config.es) {
  val local: Boolean = true
  val tempDataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  val testSettings = Settings.builder()
    .put(settings)
    .put("path.data", tempDataDir.toString)
    .put("path.home", "target/elasticsearch")
    .put("transport.type", "local")
    .put("http.enabled", "false")
    .build

  val node = new Node(testSettings).start()

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

    Try { // don't care if cleanup succeeded or failed
      FileUtils.forceDelete(tempDataDir)
    }

    super.close()
  }

  def deleteIndex(): Unit = client.admin().indices().delete(Requests.deleteIndexRequest(config.es.index))
}
