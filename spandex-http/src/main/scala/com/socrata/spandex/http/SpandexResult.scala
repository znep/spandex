package com.socrata.spandex.http

import scala.collection.JavaConverters._
import scala.util.Try

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonUtil}
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.suggest.Suggest.Suggestion
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry

import com.socrata.spandex.common.client.{FieldValue, SearchResults, SpandexFields}

case class SpandexOption(text: String, score: Option[Float])

object SpandexOption {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexOption]
}

case class SpandexResult(options: Seq[SpandexOption])

object SpandexResult extends Logging {
  implicit val jCodec = AutomaticJsonCodecBuilder[SpandexResult]

  def apply(response: Suggest): SpandexResult = {
    val suggest = response.getSuggestion[Suggestion[Entry]](SpandexFields.Suggest)
    val entries = suggest.getEntries
    val options = entries.get(0).getOptions
    val spandexOptions = options.asScala.map { opt =>
      val source = opt.getHit.getSourceAsString
      JsonUtil.parseJson[FieldValue](source) match {
        case Right(fieldValue) => Some(SpandexOption(fieldValue.rawValue, Try{Some(opt.getScore)}.getOrElse(None)))
        case Left(err) =>
          logger.error(s"Unable to parse returned suggestion '$source' as FieldValue: $err")
          None
      }
    }.flatten
    SpandexResult(spandexOptions)
  }

  /* Not yet used.
   * Transforms a search result with aggregation into spandex result options
   */
  def apply(response: SearchResults[FieldValue]): SpandexResult =
    SpandexResult(response.aggs.map { src =>
      SpandexOption(src.key, Some(src.docCount))
    })

  object Fields {
    private[this] def formatQuotedString(s: String) = "\"%s\"" format s
    val routeSuggest = "suggest"
    val routeSample = "sample"
    val paramDatasetId = "datasetId"
    val paramStageInfo = "stage"
    val paramUserColumnId = "userColumnId"
    val paramText = "text"
    val paramFuzz = "fuzz"
    val paramSize = "size"
    val options = "options"
    val optionsJson = formatQuotedString(options)
    val optionsEmptyJson = "\"%s\":[]" format options
    val text = "text"
    val textJson = formatQuotedString(text)
    val score = "score"
    val scoreJson = formatQuotedString(score)
  }
}
