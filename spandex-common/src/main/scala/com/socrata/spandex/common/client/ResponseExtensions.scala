package com.socrata.spandex.common.client

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, SimpleJsonCodecBuilder, Strategy, JsonKeyStrategy, JsonUtil}
import com.socrata.datacoordinator.secondary.LifecycleStage
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse

import scala.language.implicitConversions

@JsonKeyStrategy(Strategy.Underscore)
case class DatasetCopy(datasetId: String, copyNumber: Long, version: Long, stage: LifecycleStage) {
  def updateCopy(newVersion: Long): DatasetCopy =
    DatasetCopy(datasetId, copyNumber, newVersion, stage)

  def updateCopy(newVersion: Long, newStage: LifecycleStage): DatasetCopy =
    DatasetCopy(datasetId, copyNumber, newVersion, newStage)

  def nextCopy(newCopyNumber: Long, newVersion: Long, newStage: LifecycleStage): DatasetCopy =
    DatasetCopy(datasetId, newCopyNumber, newVersion, newStage)
}
object DatasetCopy {
  implicit val lifecycleStageCodec = new JsonDecode[LifecycleStage] with JsonEncode[LifecycleStage] {
    def decode(x: JValue): JsonDecode.DecodeResult[LifecycleStage] = {
      x match {
        case JString(stage) =>
          // Account for case differences
          LifecycleStage.values.find(_.toString.toLowerCase == stage.toLowerCase) match {
            case Some(matching) => Right(matching)
            case None           => Left(DecodeError.InvalidValue(x))
          }
        case other: JValue  => Left(DecodeError.InvalidType(JString, other.jsonType))
      }
    }

    def encode(x: LifecycleStage): JValue = JString(x.toString)
  }
  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetCopy]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FieldValue(datasetId: String, copyNumber: Long, columnId: Long, rowId: Long, value: String) {
  lazy val docId = s"$datasetId|$copyNumber|$columnId|$rowId"
  lazy val compositeId = s"$datasetId|$copyNumber|$columnId"

  // Needed for codec builder
  def this(datasetId: String,
           copyNumber: Long,
           columnId: Long,
           compositeId: String,
           rowId: Long,
           value: String) = this(datasetId, copyNumber, columnId, rowId, value)
}
object FieldValue {
  implicit val jCodec = SimpleJsonCodecBuilder[FieldValue].build(
    SpandexFields.DatasetId, _.datasetId,
    SpandexFields.CopyNumber, _.copyNumber,
    SpandexFields.ColumnId, _.columnId,
    SpandexFields.CompositeId, _.compositeId,
    SpandexFields.RowId, _.rowId,
    SpandexFields.Value, _.value
  )
}

case class SearchResults[T: JsonDecode](totalHits: Long, thisPage: Seq[T])

object ResponseExtensions {
  implicit def toExtendedResponse(response: SearchResponse): SearchResponseExtensions =
    SearchResponseExtensions(response)

  implicit def toExtendedResponse(response: GetResponse): GetResponseExtensions =
    GetResponseExtensions(response)
}

case class SearchResponseExtensions(response: SearchResponse) {
  def results[T : JsonDecode]: SearchResults[T] = {
    val hits = Option(response.getHits).map(_.getHits.toSeq).getOrElse(Seq.empty)
    val sources = hits.map { hit => Option(hit.getSourceAsString) }.flatten
    val thisPage = sources.map { source => JsonUtil.parseJson[T](source).right.get }
    val totalHits = Option(response.getHits).fold(0L)(_.totalHits)
    SearchResults(totalHits, thisPage)
  }
}

case class GetResponseExtensions(response: GetResponse) {
  def result[T : JsonDecode]: Option[T] = {
    val source = Option(response.getSourceAsString)
    source.map { s => JsonUtil.parseJson[T](s).right.get }
  }
}
