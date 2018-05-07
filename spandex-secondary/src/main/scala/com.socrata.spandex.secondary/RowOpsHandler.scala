package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.Row
import com.socrata.soql.types.{SoQLText, SoQLValue}

import com.socrata.spandex.common.client.{ColumnValue, SpandexElasticSearchClient}
import com.socrata.spandex.secondary.RowOpsHandler.columnValuesForRow

class RowOpsHandler(
    client: SpandexElasticSearchClient,
    maxValueLength: Int)
  extends SecondaryEventLogger {

  def go(datasetName: String, copyNumber: Long, ops: Seq[Operation]): Unit = {
    val columnValues: Seq[ColumnValue] = ops.flatMap { op =>
      logRowOperation(op)

      // TODO: determine when data is None in the case of updates or deletions
      op match {
        case insert: Insert =>
          columnValuesForRow(datasetName, copyNumber, insert.data, maxValueLength)
        case update: Update =>
          // decrement old column values
          val deletes = update.oldData.map { data =>
            columnValuesForRow(datasetName, copyNumber, data, maxValueLength, isInsertion = false)
          }.getOrElse(List.empty)

          // increment new column values
          val inserts = columnValuesForRow(datasetName, copyNumber, update.data, maxValueLength)
          deletes ++ inserts
        case delete: Delete =>
          // decrement deleted column values
          delete.oldData.map(data =>
            columnValuesForRow(datasetName, copyNumber, data, maxValueLength, isInsertion = false)
          ).getOrElse(List.empty)
        case _ => throw new UnsupportedOperationException(s"Row operation ${op.getClass.getSimpleName} not supported")
      }
    }

    // NOTE: row ops are already batched coming from DC, so there's no need to batch here.
    // All column values are already materialized; the ES client handles batching indexing operations.
    client.putColumnValues(datasetName, copyNumber, ColumnValue.aggregate(columnValues).toList)
  }
}

object RowOpsHandler {
  private def columnValuesForRow(
      datasetName: String,
      copyNumber: Long,
      data: Row[SoQLValue],
      maxValueLength: Int,
      isInsertion: Boolean = true)
    : Seq[ColumnValue] = {
    data.toSeq.collect {
      // Spandex only stores text columns; other column types are a no op
      case (id, value: SoQLText) =>
        val count = if (isInsertion) 1L else -1L
        ColumnValue.fromDatum(datasetName, copyNumber, (id, value), maxValueLength, count)
    }
  }
}
