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

class SpandexServlet(conf: SpandexConfig,
                     client: SpandexElasticSearchClient) extends SpandexStack with Logging {
  def index: String = conf.es.index

  val version = JsonUtil.renderJson(JObject(BuildInfo.toMap.mapValues(v => JString(v.toString))))

  def columnMap(datasetId: String, copyNum: Long, userColumnId: String): ColumnMap =
    client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"column '$userColumnId' not found"))

  get("/version") {
    contentType = ContentTypeJson
    version
  }

  get("/") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get("/health") {
    contentType = ContentTypeJson
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet()
  }

  private[this] val paramDatasetId = "datasetId"
  private[this] val paramCopyNum = "copyNum"
  private[this] val copyNumMustBeNumeric = "Copy number must be numeric"
  private[this] val paramUserColumnId = "userColumnId"
  private[this] val paramText = "text"
  private[this] val paramFuzz = "fuzz"
  private[this] val paramSize = "size"
  get(s"/suggest/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId/:$paramText") {
    contentType = ContentTypeJson
    val datasetId = params.get(paramDatasetId).get
    val copyNum = Try(params.get(paramCopyNum).get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, copyNumMustBeNumeric))
    val userColumnId = params.get(paramUserColumnId).get
    val text = params.get(paramText).get
    val fuzz = Fuzziness.build(params.getOrElse(paramFuzz, conf.suggestFuzziness))
    val size = params.get(paramSize).headOption.fold(conf.suggestSize)(_.toInt)
    logger.info(s">>> $datasetId, $copyNum, $userColumnId, $text, $fuzz, $size")

    val column = columnMap(datasetId, copyNum, userColumnId)

    logger.info(s"GET /suggest ${column.docId} :: $text / $paramFuzz:${fuzz.asInt} $paramSize:$size")
    val suggestions = client.getSuggestions(column, text, fuzz, size)
    val result = SpandexResult(suggestions)
    logger.info(s"<<< $result")
    JsonUtil.renderJson(result)
  }

  get(s"/sample/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId") {
    contentType = ContentTypeJson
    val datasetId = params.get(paramDatasetId).get
    val copyNum = Try(params.get(paramCopyNum).get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, copyNumMustBeNumeric))
    val userColumnId = params.get(paramUserColumnId).get
    val size = params.get(paramSize).headOption.map{_.toInt}.getOrElse(conf.suggestSize)
    logger.info(s">>> $datasetId, $copyNum, $userColumnId, $size")

    val column = columnMap(datasetId, copyNum, userColumnId)

    logger.info(s"GET /sample ${column.docId} / $paramSize:$size")
    val result = SpandexResult(client.getSamples(column, size))
    logger.info(s"<<< $result")
    JsonUtil.renderJson(result)
  }
}
