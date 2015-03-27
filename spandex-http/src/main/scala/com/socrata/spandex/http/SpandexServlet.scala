package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder

class SpandexServlet(conf: SpandexConfig) extends SpandexServletLike {
  def client: SpandexElasticSearchClient = new SpandexElasticSearchClient(conf.es)
  def index: String = conf.es.index
}

trait SpandexServletLike extends SpandexStack {
  def client: SpandexElasticSearchClient
  def index: String



  get("//?") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health/?"){
    // TODO
  }

  get ("/suggest/:datasetId/:copyNum/:userColumnId/:text/?") {
    val datasetId = params.get("datasetId").get
    val copyNum = params.get("copyNum").get.toLong
    val userColumnId = params.get("userColumnId").get
    val text = params.get("text").get

    val column: ColumnMap = client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(HttpStatus.SC_BAD_REQUEST, reason = "column not found"))

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
