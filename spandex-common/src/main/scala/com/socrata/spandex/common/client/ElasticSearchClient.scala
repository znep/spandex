package com.socrata.spandex.common.client

import java.io.Closeable

import com.socrata.spandex.common.ElasticSearchConfig
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.util.control.NonFatal

class ElasticSearchClient(config: ElasticSearchConfig) extends Closeable with Logging {
  Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
  val settings: Settings = ImmutableSettings.settingsBuilder()
                                            .put("cluster.name", config.clusterName)
                                            .put("client.transport.sniff", true)
                                            .put("path.conf", "esconfigs/names.txt")
                                            .build()
  val transportAddress = new InetSocketTransportAddress(config.host, config.port)
  val client: Client = new TransportClient(settings).addTransportAddress(transportAddress)

  logger.debug(s"connected elasticsearch client at $transportAddress with ${settings.toDelimitedString(',')}")

  val status = try {
    client.admin().cluster().prepareHealth().get().toString
  } catch {
    case NonFatal(e) => e.getMessage
  }
  logger.debug(s"elasticsearch cluster healthcheck $status")

  def close(): Unit = client.close()
}
