package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.Row
import com.socrata.soql.types.{SoQLText, SoQLValue}

import com.socrata.spandex.common.client.{ColumnValue, _}
import com.socrata.spandex.secondary.RowOpsHandler.columnValuesForRow

class RowOpsHandler(
    client: SpandexElasticSearchClient,
    maxValueLength: Int,
    refresh: RefreshPolicy = Eventually)
  extends SecondaryEventLogger {

  def go(datasetName: String, copyNumber: Long, ops: Seq[Operation]): Unit = {
    val t0 = System.nanoTime()
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
          delete.oldData.map { data =>
            columnValuesForRow(datasetName, copyNumber, data, maxValueLength, isInsertion = false)
          }.getOrElse(List.empty)
        case _ => throw new UnsupportedOperationException(s"Row operation ${op.getClass.getSimpleName} not supported")
      }
    }

    val aggregatedValues = ColumnValue.aggregate(columnValues).toList
    client.putColumnValues(datasetName, copyNumber, aggregatedValues, refresh)

    if(logger.underlying.isDebugEnabled()){
      val t1 = System.nanoTime()
      val listSize = columnValues.toList.size
      logger.debug(("Row Operation performed on %s Column Values, aggregated to %s " +
        "values, in %f seconds.  Aggregate handling %f values per second")
        .format(listSize, aggregatedValues.size, (t1 - t0) / 1e9, listSize / ((t1 - t0) / 1e9)))
    }
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
