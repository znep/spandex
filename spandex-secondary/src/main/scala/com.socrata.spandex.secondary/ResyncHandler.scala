package com.socrata.spandex.secondary

import com.rojoma.simplearm._
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLID, SoQLValue, SoQLType}
import com.socrata.spandex.common.client.{ColumnMap, SpandexElasticSearchClient}

case class ResyncHandler(client: SpandexElasticSearchClient) extends SecondaryEventLogger {
  def go(datasetInfo: DatasetInfo,
         copyInfo: CopyInfo,
         schema: ColumnIdMap[ColumnInfo[SoQLType]],
         rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
         batchSize: Int): Unit = {
    logResync(datasetInfo.internalName, copyInfo.copyNumber)

    // Add dataset copy
    // Don't refresh ES during resync
    logger.debug(s"adding dataset_copy $datasetInfo $copyInfo")
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
    logger.debug(s"adding text columns $textColumns")
    textColumns.foreach(client.putColumnMap(_, refresh = false))

    // Add field values for text columns
    logger.debug(s"adding field_values by row")
    insertRows(datasetInfo, copyInfo, schema, rows, batchSize)

    // TODO : Guarantee refresh before read instead of after write
    logger.debug(s"refresh")
    client.refresh()
  }

  private[this] def insertRows(datasetInfo: DatasetInfo,
                         copyInfo: CopyInfo,
                         schema: ColumnIdMap[ColumnInfo[SoQLType]],
                         rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                         batchSize: Int) = {
    // Use the system ID of each row to derive its row ID.
    // This logic is adapted from PG Secondary code in soql-postgres-adapter
    // store-pg/src/main/scala/com/socrata/pg/store/PGSecondary.scala#L415
    val systemIdColumn = schema.values.find(_.isSystemPrimaryKey).get
    def rowId(row: ColumnIdMap[SoQLValue]): RowId = {
      val rowPk = row.get(systemIdColumn.systemId).get
      new RowId(rowPk.asInstanceOf[SoQLID].value)
    }

    // Add field values for text columns
    for { iter <- rows } {
      val requests =
        for {
          row <- iter
          (id, value: SoQLText) <- row.iterator
        } yield {
          client.fieldValueIndexRequest(RowOpsHandler.fieldValueFromDatum(
            datasetInfo.internalName, copyInfo.copyNumber, rowId(row), (id, value)))
        }

      // Don't refresh ES during resync
      for { batch <- requests.grouped(batchSize) } {
        client.sendBulkRequest(batch, refresh = false)
      }
    }
  }
}
