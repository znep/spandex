package com.socrata.spandex.secondary

import com.rojoma.simplearm._
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.client._

class ResyncHandler(
    client: SpandexElasticSearchClient,
    batchSize: Int,
    maxValueLength: Int,
    refresh: RefreshPolicy = Eventually)
  extends SecondaryEventLogger {

  def go(datasetInfo: DatasetInfo,
         copyInfo: CopyInfo,
         schema: ColumnIdMap[ColumnInfo[SoQLType]],
         rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Unit = {
    logResync(datasetInfo.internalName, copyInfo.copyNumber)

    // Add dataset copy
    client.putDatasetCopy(
      datasetInfo.internalName,
      copyInfo.copyNumber,
      copyInfo.dataVersion,
      copyInfo.lifecycleStage,
      refresh)

    // Add column maps for text columns
    val textColumns =
      schema.toSeq.collect { case (id, info) if info.typ == SoQLText =>
        ColumnMap(datasetInfo.internalName, copyInfo.copyNumber, info)
      }

    textColumns.foreach(client.putColumnMap(_))

    // Delete all existing column values
    // Wait for these delete operations to be refreshed before continuing
    client.deleteColumnValuesByCopyNumber(datasetInfo.internalName, copyInfo.copyNumber, refresh = BeforeReturning)

    // Add/update column values for each row
    insertRows(datasetInfo, copyInfo, schema, rows)

    client.deleteNonPositiveCountColumnValues(datasetInfo.internalName, copyInfo.copyNumber, refresh)
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

      val datasetId = datasetInfo.internalName
      val copyNumber = copyInfo.copyNumber

      columnValues.grouped(batchSize).foreach { batch =>
        client.putColumnValues(datasetId, copyNumber, ColumnValue.aggregate(batch).toList, refresh)
      }
    }
  }
}
