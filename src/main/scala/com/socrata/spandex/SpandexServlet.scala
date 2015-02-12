package com.socrata.spandex

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import javax.servlet.http.{HttpServletResponse => HttpStatus}

import scala.concurrent.duration.Duration
import scala.concurrent.Await
import scala.util.Try

import wabisabi.{Client => ElasticsearchClient}

class SpandexServlet(esc: ElasticsearchClient) extends SpandexStack {
  private val indices = Seq("spandex")
  private val indexSettings =
    """
      |{
      |  "settings": {
      |    "index": {
      |      "number_of_shards": 2,
      |      "number_of_replicas": 1,
      |      "refresh_interval": "-1",
      |      "translog": {
      |        "flush_threshold_size": "1g"
      |      }
      |    }
      |  }
      |}
    """.stripMargin
  private def ensureIndex(index: String): String = {
    val indexResponse = Await.result(esc.verifyIndex(index), Duration("1s"))
    val resultHttpCode = indexResponse.getStatusCode
    if (resultHttpCode == HttpStatus.SC_NOT_FOUND) {
      Await.result(esc.createIndex(index, Some(indexSettings)), Duration("1s")).getResponseBody
    } else {
      indexResponse.getResponseBody
    }
  }

  private val mappingBase =
    """
      |{
      |    "%s": {
      |        "_source": {"enabled": true},
      |        "_all": {"enabled": false},
      |        "properties": {
      |            %s
      |        }
      |    }
      |}
    """.stripMargin
  private val mappingCol =
    """
      |"%s": {
      |    "type": "completion",
      |    "index_analyzer": "simple",
      |    "search_analyzer": "simple",
      |    "payloads": false,
      |    "preserve_separators": false,
      |    "preserve_position_increments": false,
      |    "max_input_length": 50
      |}
    """.stripMargin
  private def updateMapping(fourbyfour: String, column: Option[String] = None): String = {
    val _ = indices.map(ensureIndex)

    val previousMapping = Await.result(esc.getMapping(indices, Seq(fourbyfour)), Duration("1s")).getResponseBody
    val cs: List[String] = Try(new JPath(JsonReader.fromString(previousMapping)).*.*.down(fourbyfour).
      down("properties").finish.collect { case JObject(fields) => fields.keys.toList }.head).getOrElse(Nil)

    val newColumns = if (column == None || cs.contains(column)) cs else column.get :: cs
    val newMapping = mappingBase.format(fourbyfour, newColumns.map(mappingCol.format(_)).mkString(","))

    Await.result(esc.putMapping(indices, fourbyfour, newMapping), Duration("1s")).getResponseBody
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
    val response = Await.result(esc.health(level = Some("cluster")), Duration("1s"))
    response.getResponseBody
  }

  get ("/mapping"){
    Await.result(esc.getMapping(indices,Seq.empty), Duration("1s")).getResponseBody
  }

  get ("/add/:4x4/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add index routing
    updateMapping(fourbyfour)
  }

  get ("/add/:4x4/:col/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    updateMapping(fourbyfour, Some(column))
  }

  get ("/syn/:4x4"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val matchall = "{\"query\": { \"match_all\": {} } }"
    Await.result(esc.deleteByQuery(indices, Seq(fourbyfour), matchall), Duration("1d")).getResponseBody
  }

  get ("/suggest/:4x4/:col/:txt") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    val text = params.getOrElse("txt", halt(HttpStatus.SC_BAD_REQUEST))
    val query =
      """
        |{"suggest": {"text":"%s", "completion": {"field": "%s", "fuzzy": {"fuzziness": 2} } } }
      """.stripMargin.format(text, column)
    // TODO: wabisabi pull request https://github.com/gphat/wabisabi/pull/18
    indices.map(i => Await.result(esc.search(i, query), Duration("10s")).getResponseBody)
  }

  post("/ver/:4x4") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val updates = request.body
    indices.map(i => Await.result(esc.bulk(Some(i), Some(fourbyfour), updates), Duration("1d")).getResponseBody)
  }
}
