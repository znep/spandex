package com.socrata.spandex.secondary

import com.rojoma.simplearm._
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.client.{ColumnMap, ColumnValue, SpandexElasticSearchClient}

class ResyncHandler(
    client: SpandexElasticSearchClient,
    batchSize: Int,
    maxValueLength: Int)
  extends SecondaryEventLogger {

  def go(datasetInfo: DatasetInfo,
         copyInfo: CopyInfo,
         schema: ColumnIdMap[ColumnInfo[SoQLType]],
         rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Unit = {
    logResync(datasetInfo.internalName, copyInfo.copyNumber)

    // Add dataset copy
    // Don't refresh ES during resync
    client.putDatasetCopy(datasetInfo.internalName,
      copyInfo.copyNumber,
      copyInfo.dataVersion,
      copyInfo.lifecycleStage,
      refresh = false)

    // Add column maps for text columns
    val textColumns =
      schema.toSeq.collect { case (id, info) if info.typ == SoQLText =>
        ColumnMap(datasetInfo.internalName, copyInfo.copyNumber, info)
      }

    // Don't refresh ES during resync
    textColumns.foreach(client.putColumnMap(_, refresh = false))

    // Delete all existing column values
    client.deleteColumnValuesByCopyNumber(datasetInfo.internalName, copyInfo.copyNumber, false)

    // Add/update column values for each row
    insertRows(datasetInfo, copyInfo, schema, rows)
  }

  private def insertRows(
      datasetInfo: DatasetInfo,
      copyInfo: CopyInfo,
      schema: ColumnIdMap[ColumnInfo[SoQLType]],
      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]) = {
    // Add column values for text columns
    rows.foreach { iter =>
      val columnValues = for {
        row <- iter
        (id, value: SoQLText) <- row.iterator
      } yield {
        ColumnValue.fromDatum(datasetInfo.internalName, copyInfo.copyNumber, (id, value), maxValueLength)
      }

      columnValues.grouped(batchSize).foreach { batch =>
        client.putColumnValues(datasetInfo.internalName, copyInfo.copyNumber, ColumnValue.aggregate(batch).toList)
      }
    }
  }
}
