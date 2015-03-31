package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.rojoma.json.v3.ast.{JString, JObject}
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder

import scala.util.Try

class SpandexServlet(conf: SpandexConfig) extends SpandexServletLike {
  def client: SpandexElasticSearchClient = new SpandexElasticSearchClient(conf.es)
  def index: String = conf.es.index
}

trait SpandexServletLike extends SpandexStack with Logging {
  def client: SpandexElasticSearchClient
  def index: String

  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))

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

  get ("/health/?"){
    contentType = ContentTypeJson
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet()
  }

  get ("/suggest/:datasetId/:copyNum/:userColumnId/:text/?") {
    contentType = ContentTypeJson
    val datasetId = params.get("datasetId").get
    val copyNum = Try(params.get("copyNum").get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"Copy number must be numeric"))
    val userColumnId = params.get("userColumnId").get
    val text = params.get("text").get

    logger.info(s"GET /suggest $datasetId|$copyNum|$userColumnId :: $text")

    val column: ColumnMap = client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"column '$userColumnId' not found"))

    // TODO: configurable size and fuzziness
    client.getSuggestions(column, text, Fuzziness.TWO)
    // TODO: strip elasticsearch artifacts before returning suggested options and scores
  }
}
