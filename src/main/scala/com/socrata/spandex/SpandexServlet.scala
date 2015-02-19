package com.socrata.spandex

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import javax.servlet.http.{HttpServletResponse => HttpStatus}

import scala.concurrent.Await
import scala.util.Try

import wabisabi.{Client => ElasticsearchClient}

class SpandexServlet(conf: SpandexConfig) extends SpandexStack {
  private val esc: ElasticsearchClient = new ElasticsearchClient(conf.esUrl)
  private val indices = conf.indices
  private val indexSettings = conf.indexSettings
  private val mappingBase = conf.indexBaseMapping
  private val mappingCol = conf.indexColumnMapping

  private def ensureIndex(index: String): String = {
    val indexResponse = Await.result(esc.verifyIndex(index), conf.escTimeoutFast)
    val resultHttpCode = indexResponse.getStatusCode
    if (resultHttpCode != HttpStatus.SC_OK) {
      Await.result(esc.createIndex(index, Some(indexSettings)), conf.escTimeoutFast).getResponseBody
    } else {
      indexResponse.getResponseBody
    }
  }

  private def updateMapping(fourbyfour: String, column: Option[String] = None): String = {
    val _ = indices.map(ensureIndex)

    val previousMapping = Await.result(esc.getMapping(indices, Seq(fourbyfour)), conf.escTimeoutFast).getResponseBody
    val cs: List[String] = Try(new JPath(JsonReader.fromString(previousMapping)).*.*.down(fourbyfour).
      down("properties").finish.collect { case JObject(fields) => fields.keys.toList }.head).getOrElse(Nil)

    val newColumns = if (column == None || cs.contains(column)) cs else column.get :: cs
    val newMapping = mappingBase.format(fourbyfour, newColumns.map(mappingCol.format(_)).mkString(","))

    Await.result(esc.putMapping(indices, fourbyfour, newMapping), conf.escTimeoutFast).getResponseBody
  }

  get("//?") {
    // TODO: spandex getting started and/or quick reference
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

  post ("/add/:4x4/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add index routing
    updateMapping(fourbyfour)
  }

  post ("/add/:4x4/:col/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    updateMapping(fourbyfour, Some(column))
  }

  post ("/syn/:4x4"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val matchall = "{\"query\": { \"match_all\": {} } }"
    Await.result(esc.deleteByQuery(indices, Seq(fourbyfour), matchall), conf.escTimeout).getResponseBody
  }

  get ("/suggest/:4x4/:col/:txt") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    val text = params.getOrElse("txt", halt(HttpStatus.SC_BAD_REQUEST))
    val query =
      """
        |{"suggest": {"text":"%s", "completion": {"field": "%s", "fuzzy": {"fuzziness": 2} } } }
      """.stripMargin.format(text, column)
    indices.map(i => Await.result(esc.suggest(i, query), conf.escTimeoutFast).getResponseBody)
  }

  post("/ver/:4x4") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val updates = request.body
    indices.map(i => Await.result(esc.bulk(Some(i), Some(fourbyfour), updates), conf.escTimeout).getResponseBody)
  }
}
