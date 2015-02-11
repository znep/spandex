package com.socrata.spandex

import javax.servlet.http.{HttpServletResponse => HttpStatus}
import scala.concurrent.duration.Duration
import scala.concurrent.Await
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
      |        "_source": {"enabled": false},
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
      |    "max_input_length": 30720
      |}
    """.stripMargin
  private def mapping(fourbyfour: String, columns: Seq[String]): String =
     mappingBase.format(fourbyfour, columns.map(mappingCol.format(_)).mkString(","))

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

  get ("/add/:4x4/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add index routing
    indices.map(ensureIndex)
    val previousMapping = Await.result(esc.getMapping(indices, Seq(fourbyfour)), Duration("1s")).getResponseBody
    val newMapping = mapping(fourbyfour, Seq.empty)
    if (previousMapping != newMapping) {
      Await.result(esc.putMapping(indices, fourbyfour, newMapping), Duration("1s")).getResponseBody
    } else {
      """{
        | "acknowledged":true
        |}
      """.stripMargin.format(fourbyfour)
    }
  }

  get ("/add/:4x4/:col/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add mappings
    """{
      | "acknowledged":true,
      | "4x4":"%s",
      | "col":"%s"
      |}
    """.stripMargin.format(fourbyfour, column)
  }

  get ("/syn/:4x4"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch delete mappings and documents
    """{
      | "acknowledged":true,
      | "4x4":"%s"
      |}
    """.stripMargin.format(fourbyfour)
  }

  get ("/suggest/:4x4/:col/:txt") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    val querytext = params.getOrElse("txt", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch query
    val expectedanswer = """{"phrase":"NARCOTICS", "weight":1.0}"""
    """{
      | "4x4":"%s",
      | "col":"%s",
      | "text":"%s",
      | "suggestions": [%s]
      |}
    """.stripMargin.format(fourbyfour, column, querytext, expectedanswer)
  }

  post("/ver/:4x4") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add documents to index
    val updates = request.body
    """{
      | "acknowledged":true,
      | "4x4":"%s",
      | "updates:" [%s]
      |}
    """.stripMargin.format(fourbyfour, updates)
  }
}
