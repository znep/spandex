package com.socrata.spandex.common

import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._

class ElasticsearchServer(port: Int, dataMaster: Boolean = false) {
  private[this] val conf = new SpandexConfig
  private[this] val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile
  private[this] val settings = ImmutableSettings.settingsBuilder
    .put("path.data", dataDir.toString)
    .put("cluster.name", conf.clusterName)
    .put("http.port", s"$port")
    .put("node.data", s"$dataMaster")
    .put("node.master", s"$dataMaster")
    .build

  private[this] lazy val node = nodeBuilder().settings(settings).build

  def start(): Unit = {
    node.start()
  }

  def waitForReady(): Unit = node.client().admin().cluster().prepareHealth()
      .setWaitForYellowStatus().execute().actionGet(conf.clusterTimeout)

  def stop(): Unit = {
    node.close()

    try {
      FileUtils.forceDelete(dataDir)
    } catch {
      case e: Exception => // dataDir cleanup failed
    }
  }
}
