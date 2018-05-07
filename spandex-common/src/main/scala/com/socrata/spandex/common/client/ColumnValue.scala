package com.socrata.spandex.common.client

import scala.collection.mutable

import com.rojoma.json.v3.ast.{JObject, JNumber, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, JsonKeyStrategy, Strategy}
import org.apache.commons.codec.digest.DigestUtils

import com.socrata.datacoordinator.id.ColumnId
import com.socrata.soql.types.SoQLText

@JsonKeyStrategy(Strategy.Underscore)
case class ColumnValue(datasetId: String, copyNumber: Long, columnId: Long, value: String, count: Long) {
  val docId = ColumnValue.makeDocId(datasetId, copyNumber, columnId, value)
  val compositeId = ColumnValue.makeCompositeId(datasetId, copyNumber, columnId)

  def isNonEmpty: Boolean = value != null && value.trim.nonEmpty // scalastyle:ignore null
  def truncate(length: Int): ColumnValue = copy(value=value.substring(0, math.min(length, value.length)))
}

object ColumnValue {
  implicit object encode extends JsonEncode[ColumnValue] { // scalastyle:ignore object.name
    def encode(columnValue: ColumnValue): JValue =
      JObject(
        Map(
          "column_id" -> JNumber(columnValue.columnId),
          "composite_id" -> JString(columnValue.compositeId),
          "copy_number" -> JNumber(columnValue.copyNumber),
          "count" -> JNumber(columnValue.count),
          "dataset_id" -> JString(columnValue.datasetId),
          "value" -> JString(columnValue.value)
        ))
  }

  implicit val decode = AutomaticJsonDecodeBuilder[ColumnValue]

  private val ColumnValueHashBytes = 32

  private def sha128Hex(value: String): String =
    DigestUtils.sha256Hex(value).substring(0, ColumnValueHashBytes)

  // NOTE: we're hashing values for the purpose of generating IDs.
  // There is some possibility, however remote, of a collision, which would cause unexpected
  // behavior for the values in question.
  def makeDocId(datasetId: String, copyNumber: Long, columnId: Long, value: String): String =
    s"$datasetId|$copyNumber|$columnId|${sha128Hex(value)}"

  def makeCompositeId(datasetId: String, copyNumber: Long, columnId: Long): String =
    s"$datasetId|$copyNumber|$columnId"

  // NOTE: a cluster side analysis char_filter doesn't catch this one character in time.
  private def fixControlChars(value: String): String =
    value.replaceAll("\u001f", "")

  private def truncate(value: String, maxLength: Int): String =
    value.substring(0, math.min(maxLength, value.length))

  // NOTE: if we find ourselves doing more preprocessing, let's make this more extensible.
  private def preprocessValue(value: String, maxLength: Int): String =
    truncate(fixControlChars(value), maxLength)

  def fromDatum(
    datasetName: String,
    copyNumber: Long,
    datum: (ColumnId, SoQLText),
    maxValueLength: Int,
    count: Long = 1)
  : ColumnValue =
    datum match {
      case (id, value) =>
        ColumnValue(
          datasetName,
          copyNumber,
          id.underlying,
          preprocessValue(value.value, maxValueLength),
          count
        )
    }

  def aggregate(columnValues: Iterable[ColumnValue]): Iterator[ColumnValue] = {
    val columnValCountMap = mutable.Map.empty[ColumnIdValue, ColumnValue]

    columnValues.foreach { cv =>
      val colIdVal = ColumnIdValue(cv.columnId, cv.value)
      val defaultColumnVal = ColumnValue(cv.datasetId, cv.copyNumber, cv.columnId, cv.value, 0)
      val columnVal = columnValCountMap.getOrElse(colIdVal, defaultColumnVal)
      columnValCountMap += (colIdVal -> columnVal.copy(count = columnVal.count + cv.count))
    }

    columnValCountMap.valuesIterator
  }
}

