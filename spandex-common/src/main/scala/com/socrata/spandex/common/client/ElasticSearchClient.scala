package com.socrata.spandex.common.client

import java.io.Closeable
import java.net.InetAddress
import scala.util.control.NonFatal

import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import com.socrata.spandex.common.{ElasticSearchConfig, ElasticsearchClientLogger}

class ElasticSearchClient(config: ElasticSearchConfig) extends Closeable with ElasticsearchClientLogger {
  Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
  val settings: Settings = Settings.builder()
    .put("cluster.name", config.clusterName)
    .put("client.transport.sniff", true)
    .build()
  val transportAddress = new InetSocketTransportAddress(InetAddress.getByName(config.host), config.port)
  val client: Client = new PreBuiltTransportClient(settings).addTransportAddress(transportAddress)
  logClientConnected(transportAddress, settings)

  val status = try {
    client.admin().cluster().prepareHealth().get().toString
  } catch {
    case NonFatal(e) => e.getMessage
  }
  logClientHealthcheckStatus(status)

  def close(): Unit = client.close()
}
