package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import org.elasticsearch.action.admin.cluster.health.{ClusterHealthRequest, ClusterHealthRequestBuilder}
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder
import scala.util.Try

class SpandexServlet(conf: SpandexConfig) extends SpandexServletLike {
  def client: SpandexElasticSearchClient = new SpandexElasticSearchClient(conf.es)
  def index: String = conf.es.index
}

trait SpandexServletLike extends SpandexStack {
  def client: SpandexElasticSearchClient
  def index: String

  val log = org.slf4j.LoggerFactory.getLogger(getClass)

  get("//?") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health/?"){
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet()
  }

  get ("/suggest/:datasetId/:copyNum/:userColumnId/:text/?") {
    contentType = "application/json"
    val datasetId = params.get("datasetId").get
    val copyNum = Try(params.get("copyNum").get.toLong)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"Copy number must be numeric"))
    val userColumnId = params.get("userColumnId").get
    val text = params.get("text").get

    log.info(s"GET /suggest $datasetId|$copyNum|$userColumnId :: $text")

    val column: ColumnMap = client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, s"column '$userColumnId' not found"))

    val suggestion = new CompletionSuggestionFuzzyBuilder("suggest")
      .addContextField(SpandexFields.CompositeId, column.composideId)
      .setFuzziness(Fuzziness.TWO)
      .field(SpandexFields.Value)
      .text(text)
      .size(10) // scalastyle:ignore magic.number
    // TODO: configurable size and fuzziness

    val response = client.client
      .prepareSuggest(index)
      .addSuggestion(suggestion)
      .execute().actionGet()

    response.getSuggest
    // TODO: strip elasticsearch artifacts before returning suggested options and scores
  }
}
