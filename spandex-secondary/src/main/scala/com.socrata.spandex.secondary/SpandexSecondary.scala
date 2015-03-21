package com.socrata.spandex.secondary

import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.spandex.common.{ElasticSearchConfig, SpandexConfig}
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.typesafe.config.Config
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.rest.RestStatus

class SpandexSecondary(config: ElasticSearchConfig) extends SpandexSecondaryLike {
  def this(rawConfig: Config) = this(new SpandexConfig(rawConfig).es)

  val client = new SpandexElasticSearchClient(config)
  val index  = config.index
}

trait SpandexSecondaryLike extends Secondary[SoQLType, SoQLValue] with Logging {
  def client: SpandexElasticSearchClient
  def index: String

  def init(): Unit = ()
  def shutdown(): Unit = ()

  def wantsWorkingCopies: Boolean = true

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] =
    throw new NotImplementedError("Not used anywhere yet") // scalastyle:ignore multiple.string.literals

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    val result = client.deleteFieldValuesByDataset(datasetInternalName)
    checkStatus(result.status, RestStatus.OK, s"dropDataset for $datasetInternalName")
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    val result = client.deleteFieldValuesByCopyNumber(datasetInternalName, copyNumber)
    checkStatus(result.status, RestStatus.OK, s"dropCopy for $datasetInternalName copyNumber $copyNumber")
    cookie
  }

  private def checkStatus(actual: RestStatus, expected: RestStatus, description: String): Unit = {
    actual match {
      case `expected` =>
        logger.info(s"$description was successful")
      case other: RestStatus =>
        logger.info(s"$description failed with HTTP status $other")
    }
  }

  def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie, events: Events): Cookie = {
    // scalastyle:off
    println("*** SPANDEX GOT VERSION EVENTS! Woo hoo ****")
    println("Dataset internal name: " + datasetInfo.internalName)
    println("Data version: " + dataVersion)
    println("Cookie: " + cookie.getOrElse(""))
    // scalastyle:on
    val handler = new VersionEventsHandler(client)
    handler.handle(datasetInfo.internalName, dataVersion, events)

    cookie
  }

  def resync(datasetInfo: DatasetInfo,
             copyInfo: CopyInfo,
             schema: ColumnIdMap[ColumnInfo[SoQLType]],
             cookie: Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo]): Cookie = ???
}
