package com.socrata.spandex.secondary

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.{SpandexBootstrap, SpandexConfig}
import wabisabi.{Client => ElasticSearchClient}

import scala.concurrent.Await
import scala.util.Try

class SpandexSecondary(conf: SpandexConfig) extends Secondary[SoQLType, SoQLValue] {
  private val esc = new ElasticSearchClient(conf.esUrl)
  private val index = conf.index
  private val indices = List(index)
  private val indexSettings = conf.indexSettings
  private val mappingBase = conf.indexBaseMapping
  private val mappingCol = conf.indexColumnMapping
  private val bulkBatchSize = conf.bulkBatchSize

  init()

  def init(): Unit = {
    SpandexBootstrap.ensureIndex(conf)
  }

  def shutdown(): Unit = { }

  def dropDataset(datasetInternalName: String, cookie: Cookie): Unit = {
    val matchall = "{\"query\": { \"match_all\": {} } }"
    Await.result(esc.deleteByQuery(indices, Seq(datasetInternalName), matchall), conf.escTimeout).getResponseBody
  }

  @deprecated("Out of scope", since = "forever")
  def snapshots(datasetInternalName: String, cookie: Cookie): Set[Long] = Set.empty

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = cookie

  def currentCopyNumber(datasetInternalName: String, cookie: Cookie): Long = ???

  def wantsWorkingCopies: Boolean = true

  def currentVersion(datasetInternalName: String, cookie: Cookie): Long = ???

  override def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie,
                       events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    _version(datasetInfo, dataVersion, cookie, events)
  }

  def _version(secondaryDatasetInfo: DatasetInfo, newDataVersion: Long, cookie: Cookie,
               events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val fourbyfour = secondaryDatasetInfo.internalName

    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(copyInfo) => true
      case _ => false
    }

    // got working copy event
    if (wccEvents.hasNext) wccEvents.next()

    if (wccEvents.hasNext) {
      val msg = s"Got ${wccEvents.size+1} leading WorkingCopyCreated events, only support one in a version"
      throw new UnsupportedOperationException(msg)
    }

    updateMapping(fourbyfour)

    // TODO: handle version number invalid -> resync
    if (newDataVersion == -1) ???

    remainingEvents.foreach {
      case Truncated => dropCopy(fourbyfour, truncate = true)
      case ColumnCreated(secColInfo) => updateMapping(fourbyfour, Some(secColInfo.systemId.underlying.toString))
      case ColumnRemoved(secColInfo) => { /* TODO: remove column */ }
      case RowDataUpdated(ops) => ???
      case DataCopied => ??? // working copy
      case WorkingCopyPublished => ??? // working copy
      case SnapshotDropped(info) => dropCopy(fourbyfour)
      case WorkingCopyDropped => dropCopy(fourbyfour)
      case _ => "k thx bye"
    }

    // TODO: set new version number

    cookie
  }

  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]],
                      cookie: Cookie, rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Cookie = {
    _resync(datasetInfo, copyInfo, schema, cookie, rows)
  }

  private def _resync(secondaryDatasetInfo: DatasetInfo, secondaryCopyInfo: CopyInfo,
                      newSchema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Cookie = {
    val fourbyfour = secondaryDatasetInfo.internalName
    dropCopy(fourbyfour)
    updateMapping(fourbyfour)

    val columns = newSchema.filter((_, i) => i.typ == SoQLText).keySet
    columns.foreach { i => updateMapping(fourbyfour, Some(i.underlying.toString)) }

    val sysIdCol = newSchema.values.find(_.isSystemPrimaryKey == true).get.systemId

    for (iter <- rows) {
      iter.grouped(bulkBatchSize).foreach { bi =>
        for (row: ColumnIdMap[SoQLValue] <- bi) {
          val docId = row(sysIdCol)
          val kvp = columns.foreach { i =>
            (i, row.getOrElse(i, ""))
          }
        }
      }
    }

    cookie
  }

  private def updateMapping(fourbyfour: String, column: Option[String] = None): String = {
    val previousMapping = Await.result(esc.getMapping(indices, Seq(fourbyfour)), conf.escTimeoutFast).getResponseBody
    val cs: List[String] = Try(new JPath(JsonReader.fromString(previousMapping)).*.*.down(fourbyfour).
      down("properties").finish.collect { case JObject(fields) => fields.keys.toList }.head).getOrElse(Nil)

    val newColumns = if (column == None || cs.contains(column)) cs else column.get :: cs
    val newMapping = mappingBase.format(fourbyfour, newColumns.map(mappingCol.format(_)).mkString(","))

    Await.result(esc.putMapping(indices, fourbyfour, newMapping), conf.escTimeoutFast).getResponseBody
  }

  private def dropCopy(fourbyfour: String, truncate: Boolean = false): Unit = {
    val matchall = "{\"query\": { \"match_all\": {} } }"
    Await.result(esc.deleteByQuery(indices, Seq(fourbyfour), matchall), conf.escTimeout).getResponseBody
    if (!truncate) {
      // TODO: remove dataset mapping as well
    }
  }
}
