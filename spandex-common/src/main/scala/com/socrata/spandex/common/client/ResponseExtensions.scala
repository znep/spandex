package com.socrata.spandex.common.client

import scala.collection.mutable
import scala.language.implicitConversions

import com.rojoma.json.v3.ast.{JObject, JNumber, JString, JValue}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonDecodeBuilder, JsonKeyStrategy, JsonUtil, Strategy} // scalastyle:ignore line.size.limit

import com.socrata.datacoordinator.secondary.{ColumnInfo, LifecycleStage, Row}
import com.socrata.soql.types.SoQLText
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.search.SearchHit


@JsonKeyStrategy(Strategy.Underscore)
case class DatasetCopy(datasetId: String, copyNumber: Long, version: Long, stage: LifecycleStage) {
  val docId = DatasetCopy.makeDocId(datasetId, copyNumber)
}

object DatasetCopy {
  implicit val lifecycleStageCodec = new JsonDecode[LifecycleStage] with JsonEncode[LifecycleStage] {
    def decode(x: JValue): JsonDecode.DecodeResult[LifecycleStage] = {
      x match {
        case JString(stage) =>
          // Account for case differences
          LifecycleStage.values.find(_.toString.toLowerCase == stage.toLowerCase) match {
            case Some(matching) => Right(matching)
            case None => Left(DecodeError.InvalidValue(x))
          }
        case _ => Left(DecodeError.InvalidType(JString, x.jsonType))
      }
    }

    def encode(x: LifecycleStage): JValue = JString(x.toString)
  }

  implicit val jCodec = AutomaticJsonCodecBuilder[DatasetCopy]

  def makeDocId(datasetId: String, copyNumber: Long): String = s"$datasetId|$copyNumber"
}

@JsonKeyStrategy(Strategy.Underscore)
case class ColumnMap(
    datasetId: String,
    copyNumber: Long,
    systemColumnId: Long,
    userColumnId: String) {
  val docId = ColumnMap.makeDocId(datasetId, copyNumber, userColumnId)
  val compositeId = ColumnMap.makeCompositeId(datasetId, copyNumber, systemColumnId)
}

object ColumnMap {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnMap]

  def apply(datasetId: String, copyNumber: Long, columnInfo: ColumnInfo[_]): ColumnMap =
    this(datasetId, copyNumber, columnInfo.systemId.underlying, columnInfo.id.underlying)

  def makeDocId(datasetId: String, copyNumber: Long, userColumnId: String): String =
    s"$datasetId|$copyNumber|$userColumnId"

  def makeCompositeId(datasetId: String, copyNumber: Long, systemColumnId: Long): String =
    s"$datasetId|$copyNumber|$systemColumnId"
}

@JsonKeyStrategy(Strategy.Underscore)
case class CompositeId(compositeId: String)

object CompositeId {
  implicit val codec = AutomaticJsonCodecBuilder[CompositeId]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SuggestWithContext(input: Seq[String], contexts: CompositeId)

object SuggestWithContext {
  implicit val codec = AutomaticJsonCodecBuilder[SuggestWithContext]
}

case class ScoredResult[T](result: T, score: Float)

case class SearchResults[T: JsonDecode](totalHits: Long, thisPage: Seq[ScoredResult[T]])

object ResponseExtensions {
  implicit def toExtendedResponse(response: SearchResponse): SearchResponseExtensions =
    SearchResponseExtensions(response)

  implicit def toExtendedResponse(response: GetResponse): GetResponseExtensions =
    GetResponseExtensions(response)

  implicit def toExtendedResponse(response: BulkResponse): BulkResponseExtensions =
    BulkResponseExtensions(response)
}

case class SearchResponseExtensions(response: SearchResponse) {
  def results[T : JsonDecode](score: Option[(T) => Float] = None): SearchResults[T] = {
    val hits = Option(response.getHits).fold(Seq.empty[SearchHit])(_.getHits.toSeq)

    val thisPage = hits.flatMap { hit =>
      Option(hit.getSourceAsString).flatMap { source =>
        JsonUtil.parseJson[T](source) match {
          case Right(result) =>
            Some(ScoredResult(result, score.map(fn => fn(result)).getOrElse(hit.getScore)))
          case Left(error) => None
        }
      }
    }

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

case class BulkResponseAcknowledgement(
    deletions: Map[String, Int],
    updates: Map[String, Int],
    creations: Map[String, Int])

object BulkResponseAcknowledgement {
  def empty: BulkResponseAcknowledgement = BulkResponseAcknowledgement(Map.empty, Map.empty, Map.empty)

  def apply(bulkResponse: BulkResponse): BulkResponseAcknowledgement = {
    val deletions = mutable.Map[String, Int]()
    val updates = mutable.Map[String, Int]()
    val creations = mutable.Map[String, Int]()

    bulkResponse.getItems.toList.foreach { itemResponse =>
      val countsToUpdate = itemResponse.getOpType match {
        case OpType.DELETE => deletions
        case OpType.UPDATE => updates
        case OpType.CREATE => creations
        case _ => mutable.Map.empty[String, Int]
      }

      countsToUpdate += (itemResponse.getType -> (countsToUpdate.getOrElse(itemResponse.getType, 0) + 1))
    }

    BulkResponseAcknowledgement(deletions.toMap, updates.toMap, creations.toMap)
  }
}

case class BulkResponseExtensions(response: BulkResponse) {
  def deletions: Map[String, Int] = BulkResponseAcknowledgement(response).deletions
}

case class DatasetConfig(
    datasetCopy: DatasetCopy,
    columnMap: ColumnMap,
    pathToData: String)

object DatasetConfig {
  implicit val jCodec = AutomaticJsonCodecBuilder[ColumnMap]
}
