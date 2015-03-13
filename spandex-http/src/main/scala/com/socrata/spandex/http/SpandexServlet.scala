package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

class SpandexServlet(conf: SpandexConfig, esPort: Int) extends SpandexStack {
  private[this] val esc: ElasticsearchClient = new ElasticsearchClient(conf.esUrl(esPort))
  private[this] val index = conf.index
  private[this] val indices = List(index)
  private[this] val indexSettings = conf.indexSettings
  private[this] val mappingBase = conf.indexBaseMapping
  private[this] val mappingCol = conf.indexColumnMapping

  get("//?") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health/?"){
    val response = Await.result(esc.health(level = Some("cluster")), conf.escTimeoutFast)
    response.getResponseBody
  }

  get ("/mapping"){
    Await.result(esc.getMapping(indices,Seq.empty), conf.escTimeoutFast).getResponseBody
  }

  get ("/suggest/:4x4/:col/:txt") {
    val fourbyfour: String = params.getOrElse("4x4", "")
    val column: String = params.getOrElse("col", "")
    val text: String = params.getOrElse("txt", "")
    val query =
      """
        |{"suggest": {"text":"%s", "completion": {"field": "%s", "fuzzy": {"fuzziness": 2} } } }
      """.stripMargin.format(text, column)
    Await.result(esc.suggest(index, query), conf.escTimeoutFast).getResponseBody
  }
}
