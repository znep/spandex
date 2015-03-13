package com.socrata.spandex.secondary

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common.{SpandexBootstrap, SpandexConfig}
import org.joda.time.DateTime
import wabisabi.{Client => ElasticSearchClient}

import scala.concurrent.Await
import scala.util.Try

class SpandexSecondary(conf: SpandexConfig) extends Secondary[SoQLType, SoQLValue] {
  private[this] val esc = new ElasticSearchClient(conf.esUrl)
  private[this] val escTimeout = conf.escTimeout
  private[this] val escTimeoutFast = conf.escTimeoutFast
  private[this] val index = conf.index
  private[this] val indices = List(index)
  private[this] val mappingBase = conf.indexBaseMapping
  private[this] val mappingCol = conf.indexColumnMapping
  private[this] val bulkBatchSize = conf.bulkBatchSize
  private[this] val matchAll = "{\"query\": { \"match_all\": {} } }"
  private[this] val hits = "hits"

  private[this] val quotesRegex = "^[\"]*([^\"]+)[\"]*$".r
  private[this] def trimQuotes(s: String): String = s match {case quotesRegex(inside) => inside}

  init()

  def init(): Unit = {
    SpandexBootstrap.ensureIndex(conf)
  }

  def shutdown(): Unit = { }

  // delete the whole dataset
  def dropDataset(fxf: String, cookie: Cookie): Unit = {
    // drop all the working copies
    snapshots(fxf, cookie).foreach { copy =>
      dropCopy(fxf, copy, cookie)
    }
    // delete the list of working copies
    Await.result(esc.deleteByQuery(indices, List(fxf), matchAll), escTimeoutFast).getResponseBody
  }

  // return list of working copies' IDs
  def snapshots(fxf: String, cookie: Cookie): Set[Long] = doSnapshots(fxf).map(s => s._1)

  private[this] def doSnapshots(fxf: String): Set[(Long, Long)] = {
    val r = Await.result(esc.search(index, matchAll, `type` = Some(fxf)), escTimeout).getResponseBody
    val t: Try[Set[(Long, Long)]] = Try {
      new JPath(JsonReader.fromString(r)).down(hits).down(hits).*.finish.collect {
        case JObject(fields) => {
          val id = trimQuotes(fields("_id").toString())
          val src = fields("_source").toString()

          val version: Long = Try {
            new JPath(JsonReader.fromString(src)).down("truthVersion").finish.collect {
              case s: JValue => trimQuotes(s.toString()).toLong
            }.head
          }.getOrElse(-1)

          (id.toLong, version)
        }
      }.toSet
    }
    t.getOrElse(Set.empty)
  }

  // delete a working copy
  def dropCopy(fxf: String, copyNumber: Long, cookie: Cookie): Cookie = {
    doDropCopy(fxf, copyNumber.toString)
    cookie
  }

  // return the current working copy ID
  // TODO: figure out which working copy is current
  def currentCopyNumber(fxf: String, cookie: Cookie): Long =
    snapshots(fxf, cookie).headOption.getOrElse(-1)

  def wantsWorkingCopies: Boolean = true

  // get the version of the current working copy
  def currentVersion(fxf: String, cookie: Cookie): Long = {
    val copy = currentCopyNumber(fxf, cookie)
    doSnapshots(fxf).find(s => s._1 == copy).map(s => s._2).getOrElse(-1)
  }

  override def version(datasetInfo: DatasetInfo, dataVersion: Long, cookie: Cookie,
                       events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    doVersion(datasetInfo, dataVersion, cookie, events)
  }

  private[this] def doVersion(datasetInfo: DatasetInfo, newDataVersion: Long, // scalastyle:ignore cyclomatic.complexity
                cookie: Cookie, events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    val fxf = datasetInfo.internalName
    val copies = doSnapshots(fxf).toList

    if (newDataVersion < 0) throw new UnsupportedOperationException(s"version $newDataVersion invalid")
    copies.map {
      case (_, ver) => if (newDataVersion <= ver) {
        throw new UnsupportedOperationException(s"version $newDataVersion already assigned")
      }
    }

    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(_) => true
      case _ => false
    }

    // got working copy event
    if (wccEvents.hasNext) {
      val WorkingCopyCreated(copyInfo) = wccEvents.next()
      println(s"working copy $fxf $copyInfo")
      if (copyInfo.copyNumber < 0) throw new UnsupportedOperationException(s"copy $copyInfo invalid")
      copies.map {
        case (id, _) => if (copyInfo.copyNumber <= id) {
          throw new UnsupportedOperationException(s"copy $id already assigned")
        }
      }
      doUpdateMapping(fxf, copyInfo.copyNumber.toString)
    }

    if (wccEvents.hasNext) {
      val msg = s"Got ${wccEvents.size + 1} leading WorkingCopyCreated events, only support one in a version"
      throw new UnsupportedOperationException(msg)
    }

    val copy = currentCopyNumber(fxf, cookie).toString

    // TODO: elasticsearch add index routing
    remainingEvents.foreach {
      case Truncated => doDropCopy(fxf, copy, truncate = true)
      case ColumnCreated(colInfo) => doUpdateMapping(fxf, copy, Some(colInfo.systemId.underlying.toString))
      case ColumnRemoved(colInfo) => { /* TODO: remove column */ }
      case LastModifiedChanged(lastModified) => {
        Await.result(
          esc.index(conf.index, fxf, Some(copy), "{\"truthUpdate\":\"%s\"}".format(lastModified)),
          conf.escTimeout
        )
      }
      case WorkingCopyDropped => doDropCopy(fxf, copy)
      case DataCopied => { /* TODO: figure out what is required */ }
      case SnapshotDropped(info) => doDropCopy(fxf, copy)
      case WorkingCopyPublished => { /* TODO: pay attention to working copy lifecycle */ }
      case RowDataUpdated(ops) => ???
      case i: Any => throw new UnsupportedOperationException(s"event not supported: '$i'")
    }

    Await.result(esc.index(conf.index, fxf, Some(copy),
        "{\"truthVersion\":\"%d\", \"truthUpdate\":\"%d\"}".format(newDataVersion, DateTime.now().getMillis)),
      conf.escTimeoutFast
    )

    cookie
  }

  override def resync(datasetInfo: DatasetInfo, copyInfo: CopyInfo, schema: ColumnIdMap[ColumnInfo[SoQLType]],
                      cookie: Cookie, rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
                      rollups: Seq[RollupInfo]): Cookie = {
    doResync(datasetInfo, copyInfo, schema, cookie, rows)
  }

  private[this] def doResync(datasetInfo: DatasetInfo, copyInfo: CopyInfo,
                      newSchema: ColumnIdMap[ColumnInfo[SoQLType]], cookie: Cookie,
                      rows: Managed[Iterator[ColumnIdMap[SoQLValue]]]): Cookie = {
    val fxf = datasetInfo.internalName
    val copy = copyInfo.copyNumber.toString
    doDropCopy(fxf, copy)
    doUpdateMapping(fxf, copy)

    val columns = newSchema.filter((_, i) => i.typ == SoQLText).keySet
    columns.foreach { i => doUpdateMapping(fxf, copy, Some(i.underlying.toString)) }

    val sysIdCol = newSchema.values.find(_.isSystemPrimaryKey).
      getOrElse(throw new RuntimeException("missing system primary key")).systemId

    for {iter <- rows} {
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

  private[this] def doUpdateMapping(fxf: String, copy: String, column: Option[String] = None): String = {
    val fxfcopy = s"$fxf-$copy"

    // add to dataset list of working copies
    if (column.isEmpty) {
      Await.result(esc.index(index, fxf, Some(copy), s"""{"copy":"$copy"}"""), escTimeoutFast).getResponseBody
    }

    val previousMapping = Await.result(esc.getMapping(indices, Seq(fxfcopy)), escTimeoutFast).getResponseBody
    val cs: List[String] = Try(new JPath(JsonReader.fromString(previousMapping)).*.*.down(fxfcopy).
      down("properties").finish.collect { case JObject(fields) => fields.keys.toList }.head).getOrElse(Nil)

    val newColumns = column match {
      case Some(c) if !cs.contains(c) => c::cs
      case _ => cs
    }
    val newMapping = mappingBase.format(fxfcopy, newColumns.map(mappingCol.format(_)).mkString(","))

    // put mapping
    Await.result(esc.putMapping(indices, fxfcopy, newMapping), escTimeoutFast).getResponseBody
  }

  private[this] def doDropCopy(fxf: String, copy: String, truncate: Boolean = false): Unit = {
    val fxfcopy = s"$fxf-$copy"
    // delete the documents
    Await.result(esc.deleteByQuery(indices, Seq(fxfcopy), matchAll), escTimeout).getResponseBody
    if (!truncate) {
      // TODO: remove dataset mapping as well
      // delete from dataset list of working copies
      Await.result(esc.delete(index, fxf, copy), escTimeout).getResponseBody
    }
  }
}
