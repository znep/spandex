package com.socrata.spandex.common

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

object SpandexBootstrap {
  def ensureIndex(conf: SpandexConfig, port: Int): String = {
    val esc = new ElasticsearchClient(conf.esUrl(port))
    val indexResponse = Await.result(esc.verifyIndex(conf.es.index), conf.escTimeoutFast)
    val resultHttpCode = indexResponse.getStatusCode
    if (resultHttpCode != HttpStatus.SC_OK) {
      Await.result(esc.createIndex(conf.es.index, Some(conf.indexSettings)), conf.escTimeoutFast).getResponseBody
      Await.result(esc.putMapping(Seq(conf.es.index), conf.es.fieldValueMapping.mappingType,
        conf.es.fieldValueMapping.mappingProperties), conf.escTimeoutFast).getResponseBody
      Await.result(esc.putMapping(Seq(conf.es.index), conf.es.datasetCopyMapping.mappingType,
        conf.es.datasetCopyMapping.mappingProperties), conf.escTimeoutFast).getResponseBody
    } else {
      indexResponse.getResponseBody
    }
  }
}
