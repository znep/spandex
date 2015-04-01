package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.rojoma.json.v3.ast.{JObject, JString}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.common.unit.Fuzziness

import scala.util.Try

class SpandexServlet(val conf: SpandexConfig) extends SpandexServletLike {
  def client: SpandexElasticSearchClient = new SpandexElasticSearchClient(conf.es)
  def index: String = conf.es.index
}

trait SpandexServletLike extends SpandexStack with Logging {
  val conf: SpandexConfig
  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))
  def client: SpandexElasticSearchClient
  def index: String

  def columnMap(datasetId: String, copyNum: Long, userColumnId: String): ColumnMap = {
    val column: ColumnMap = client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"column '$userColumnId' not found"))
    column
  }

  get("/version") {
    contentType = ContentTypeJson
    version
  }

  get("//?") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get("/health/?") {
    contentType = ContentTypeJson
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet()
  }

  get("/suggest/:datasetId/:copyNum/:userColumnId/:text/?") {
    contentType = ContentTypeJson
    val datasetId = params.get("datasetId").get
    val copyNum = Try(params.get("copyNum").get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"Copy number must be numeric"))
    val userColumnId = params.get("userColumnId").get
    val text = params.get("text").get
    val fuzz = Fuzziness.build(params.getOrElse("fuzz", conf.suggestFuzziness))
    val size = params.get("size").headOption.map{_.toInt}.getOrElse(conf.suggestSize)

    val column = columnMap(datasetId, copyNum, userColumnId)

    logger.info(s"GET /suggest ${column.docId} :: $text / fuzz:$fuzz size:$size")
    client.getSuggestions(column, text, fuzz, size)
    // TODO: strip elasticsearch artifacts before returning suggested options and scores
  }

  get("/sample/:datasetId/:copyNum/:userColumnId/?") {
    contentType = ContentTypeJson
    val datasetId = params.get("datasetId").get
    val copyNum = Try(params.get("copyNum").get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"Copy number must be numeric"))
    val userColumnId = params.get("userColumnId").get
    val size = params.get("size").headOption.map{_.toInt}.getOrElse(conf.suggestSize)

    val column = columnMap(datasetId, copyNum, userColumnId)

    logger.info(s"GET /sample ${column.docId} / size:$size")
    client.getSamples(column, size).thisPage.map { src =>
      """{
        |"text" : "%s",
        |"score" : "%s"
        |}""".stripMargin.format(src.value, src)
    }.mkString("{\"options\" : [", ",", "]}")
  }
}
