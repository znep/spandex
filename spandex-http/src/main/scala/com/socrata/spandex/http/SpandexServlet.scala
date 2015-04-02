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

  private[this] val routeSuggest = "suggest"
  private[this] val paramDatasetId = "datasetId"
  private[this] val paramCopyNum = "copyNum"
  private[this] val paramUserColumnId = "userColumnId"
  private[this] val paramText = "text"
  get(s"/$routeSuggest/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId/:$paramText") {
    suggest { (col, text, fuzz, size) =>
      SpandexResult(client.getSuggestions(col, size, text, fuzz, conf.suggestFuzzLength, conf.suggestFuzzPrefix))
    }
  }
  get(s"/$routeSuggest/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId") {
    val sampleText = "a"
    val sampleFuzz = Fuzziness.ONE
    val sampleFuzzLen = 0
    val sampleFuzzPre = 0
    suggest { (col, _, _, size) =>
      SpandexResult(client.getSuggestions(col, size, sampleText, sampleFuzz, sampleFuzzLen, sampleFuzzPre))
    }
  }

  def suggest(f: (ColumnMap, String, Fuzziness, Int) => SpandexResult): String = {
    val copyNumMustBeNumeric = "Copy number must be numeric"
    val paramFuzz = "fuzz"
    val paramSize = "size"

    contentType = ContentTypeJson
    val datasetId = params.get(paramDatasetId).get
    val copyNum = Try(params.get(paramCopyNum).get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, copyNumMustBeNumeric))
    val userColumnId = params.get(paramUserColumnId).get
    val text = params.get(paramText).getOrElse("")
    val fuzz = Fuzziness.build(params.getOrElse(paramFuzz, conf.suggestFuzziness))
    val size = params.get(paramSize).headOption.fold(conf.suggestSize)(_.toInt)
    logger.info(s">>> $datasetId, $copyNum, $userColumnId, $text, $fuzz, $size")

    val column = columnMap(datasetId, copyNum, userColumnId)
    logger.info(s"found column $column")

    val result = f(column, text, fuzz, size)
    logger.info(s"<<< $result")
    JsonUtil.renderJson(result)
  }

  /* Not yet used.
   * sample endpoint exposes query by column with aggregation on doc count
   */
  get(s"/sample/:$paramDatasetId/:$paramCopyNum/:$paramUserColumnId") {
    suggest { (col, _, _, size) => SpandexResult(client.getSamples(col, size)) }
  }
}
