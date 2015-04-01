package com.socrata.spandex.http

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.spandex.common.client.{FieldValue, SearchResults}
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.suggest.Suggest.Suggestion
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry

import scala.collection.JavaConversions._
import scala.util.Try

case class SpandexOption(text: String, score: Option[Float])

object SpandexOption {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexOption]
}

case class SpandexResult(options: Seq[SpandexOption])

object SpandexResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexResult]

  def fromSuggest(response: Suggest): SpandexResult = {
    val suggest = response.getSuggestion[Suggestion[Entry]]("suggest")
    val entries = suggest.getEntries
    val options = entries.get(0).getOptions
    SpandexResult(options.map { a =>
      SpandexOption(a.getText.string(), Try{Some(a.getScore)}.getOrElse(None))
    })
  }

  def fromSearch(response: SearchResults[FieldValue]): SpandexResult =
    SpandexResult(response.thisPage.map { src =>
      SpandexOption(src.value, None)
    })
}
