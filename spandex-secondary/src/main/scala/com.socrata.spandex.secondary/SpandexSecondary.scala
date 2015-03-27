package com.socrata.spandex.secondary

import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.id.RowId
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.spandex.common.{SpandexBootstrap, ElasticSearchConfig, SpandexConfig}
import com.socrata.spandex.common.client.{ColumnMap, SpandexElasticSearchClient}
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging

class SpandexSecondary(config: ElasticSearchConfig) extends SpandexSecondaryLike {
  def this(rawConfig: Config) = this(new SpandexConfig(rawConfig).es)

  val client = new SpandexElasticSearchClient(config)
  val index  = config.index
  val batchSize = config.dataCopyBatchSize

  init(config)
}

trait SpandexSecondaryLike extends Secondary[SoQLType, SoQLValue] with Logging {
  def client: SpandexElasticSearchClient
  def index: String
  def batchSize: Int

  def init(config: ElasticSearchConfig): Unit = {
    SpandexBootstrap.ensureIndex(config, client)
  }
  def shutdown(): Unit = ()

  def wantsWorkingCopies: Boolean = true

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    client.deleteFieldValuesByDataset(datasetInternalName)
    client.deleteColumnMapsByDataset(datasetInternalName)
    client.deleteDatasetCopiesByDataset(datasetInternalName)
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    client.deleteFieldValuesByCopyNumber(datasetInternalName, copyNumber)
    client.deleteColumnMapsByCopyNumber(datasetInternalName, copyNumber)
    client.deleteDatasetCopy(datasetInternalName, copyNumber)
    cookie
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Iterator[Event]): Cookie = {
    val handler = new VersionEventsHandler(client)
    handler.handle(datasetInfo.internalName, dataVersion, events)
    cookie
  }

  def resync(datasetInfo: DatasetInfo,
             copyInfo: CopyInfo,
             schema: ColumnIdMap[ColumnInfo[SoQLType]],
             cookie: Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo]): Cookie = {
    // Add dataset copy
    client.putDatasetCopy(datasetInfo.internalName,
                          copyInfo.copyNumber,
                          copyInfo.dataVersion,
                          copyInfo.lifecycleStage)

    // Add column maps for text columns
    val textColumns =
      schema.toSeq.collect { case (id, info) if info.typ == SoQLText =>
        ColumnMap(datasetInfo.internalName, copyInfo.copyNumber, info)
      }
    textColumns.foreach(client.putColumnMap)

    // Use the system ID of each row to derive its row ID.
    // This logic is adapted from PG Secondary code in soql-postgres-adapter
    // store-pg/src/main/scala/com/socrata/pg/store/PGSecondary.scala#L415
    val systemIdColumn = schema.values.find(_.isSystemPrimaryKey).get
    def getRowId(row: ColumnIdMap[SoQLValue]): RowId = {
      val rowPk = row.get(systemIdColumn.systemId).get
      new RowId(rowPk.asInstanceOf[SoQLID].value)
    }

    // Add field values for text columns
    for {
      iter <- rows
      batch <- iter.grouped(batchSize)
      row <- batch
    } {
      val requests = row.toSeq.collect {
        case (id, value: SoQLText) =>
          client.getIndexRequest(RowOpsHandler.fieldValueFromDatum(
            datasetInfo.internalName, copyInfo.copyNumber, getRowId(row), (id, value)))
      }
      client.sendBulkRequest(requests)
    }

    cookie
  }
}
