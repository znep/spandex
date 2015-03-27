package com.socrata.spandex.common.client

import java.io.Closeable

import com.socrata.spandex.common.ElasticSearchConfig
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{Settings, ImmutableSettings}
import org.elasticsearch.common.transport.InetSocketTransportAddress

class ElasticSearchClient(config: ElasticSearchConfig) extends Closeable {
  Thread.currentThread().setContextClassLoader(this.getClass.getClassLoader)
  val settings: Settings = ImmutableSettings.settingsBuilder()
                                            .put("cluster.name", config.clusterName)
                                            .put("client.transport.sniff", true)
                                            .put("path.conf", "esconfigs/names.txt")
                                            .build()
  val transportAddress = new InetSocketTransportAddress(config.host, config.port)
  val client: Client = new TransportClient(settings).addTransportAddress(transportAddress)

  def close(): Unit = client.close()
}
