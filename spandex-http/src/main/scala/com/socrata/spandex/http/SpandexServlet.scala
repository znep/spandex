package com.socrata.spandex.http

import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder
import org.scalatra.ErrorHandler

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
    val datasetId = params.getOrElse("datasetId", "")
    val copyNum = params.getOrElse("copyNum", "0").toLong
    val userColumnId = params.getOrElse("userColumnId", "")
    val text = params.getOrElse("text", "")

    val systemColumnId = client.getColumnMap(datasetId, copyNum, userColumnId)
      .getOrElse(halt(reason = "column not found")).systemColumnId

    val suggestion = new CompletionSuggestionFuzzyBuilder("suggest")
      .addContextField("composite_id", s"$datasetId|$copyNum|$systemColumnId")
      .setFuzziness(Fuzziness.TWO)
      .field("value")
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
