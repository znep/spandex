package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

class SpandexServlet(conf: SpandexConfig) extends SpandexStack {
  private val esc: ElasticsearchClient = new ElasticsearchClient(conf.esUrl)
  private val index = conf.index
  private val indices = List(index)
  private val indexSettings = conf.indexSettings
  private val mappingBase = conf.indexBaseMapping
  private val mappingCol = conf.indexColumnMapping

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
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    val text = params.getOrElse("txt", halt(HttpStatus.SC_BAD_REQUEST))
    val query =
      """
        |{"suggest": {"text":"%s", "completion": {"field": "%s", "fuzzy": {"fuzziness": 2} } } }
      """.stripMargin.format(text, column)
    Await.result(esc.suggest(index, query), conf.escTimeoutFast).getResponseBody
  }
}
