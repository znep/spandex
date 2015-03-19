
package com.socrata.spandex.secondary

import com.rojoma.json.v3.ast.{JObject, JValue}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.simplearm.Managed
import com.socrata.datacoordinator.common.soql.SoQLRep
import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.datacoordinator.secondary.Secondary.Cookie
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types._
import com.socrata.soql.types.obfuscation.CryptProvider
import com.socrata.spandex.common.{ElasticSearchConfig, SpandexBootstrap, SpandexConfig}
import com.typesafe.config.Config
import org.joda.time.DateTime

import scala.concurrent.Await
import scala.util.Try
import com.typesafe.scalalogging.slf4j.Logging
import com.socrata.spandex.common.client.ElasticSearchClient
import org.elasticsearch.rest.RestStatus

class SpandexSecondary(config: ElasticSearchConfig) extends SpandexSecondaryLike {
  def this(rawConfig: Config) = this(new SpandexConfig(rawConfig).es)

  val client = new ElasticSearchClient(config)
  val index  = config.index
}

trait SpandexSecondaryLike extends Secondary[SoQLType, SoQLValue] with Logging {
  def client: ElasticSearchClient
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
    val result = client.deleteByDataset(datasetInternalName)
    checkStatus(result.status, RestStatus.OK, s"dropDataset for $datasetInternalName")
  }

  def dropCopy(datasetInternalName: String, copyNumber: Long, cookie: Cookie): Cookie = {
    val result = client.deleteByCopyId(datasetInternalName, copyNumber)
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

  def version(datasetInfo: DatasetInfo,
              dataVersion: Long,
              cookie: Cookie,
              events: Iterator[Event[SoQLType, SoQLValue]]): Cookie = {
    events.foreach(e => println(e.getClass)) // scalastyle:off
    cookie
  }

  def resync(datasetInfo: DatasetInfo,
             copyInfo: CopyInfo,
             schema: ColumnIdMap[ColumnInfo[SoQLType]],
             cookie: Cookie,
             rows: Managed[Iterator[ColumnIdMap[SoQLValue]]],
             rollups: Seq[RollupInfo]): Cookie = ???
}


class SpandexSecondaryOld(conf: SpandexConfig, esPort: Int) extends Secondary[SoQLType, SoQLValue] {
  def this(rawConf: Config) = {
    this(new SpandexConfig(rawConf), new SpandexConfig(rawConf).es.port)
  }

  private[this] val esc = new wabisabi.Client(conf.esUrl(esPort))
  private[this] val escTimeout = conf.escTimeout
  private[this] val escTimeoutFast = conf.escTimeoutFast
  private[this] val index = conf.es.index
  private[this] val indices = List(index)
  private[this] val mappingBase = "" // conf.indexBaseMapping
  private[this] val mappingCol = conf.indexColumnMapping
  private[this] val bulkBatchSize = conf.bulkBatchSize
  private[this] val matchAll = "{\"query\": { \"match_all\": {} } }"
  private[this] val hits = "hits"
  private[this] val comma: String = ","

  private[this] val quotesRegex = "^[\"]*([^\"]+)[\"]*$".r
  private[this] def trimQuotes(s: String): String = s match {case quotesRegex(inside) => inside}

  init()

  def init(): Unit = {
    SpandexBootstrap.ensureIndex(conf, esPort)
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

    if (newDataVersion < 0) throw new UnsupportedOperationException(s"version invalid: $newDataVersion")
    copies.map {
      case (_, ver) => if (newDataVersion <= ver) {
        throw new UnsupportedOperationException(s"version already assigned: $newDataVersion")
      }
    }

    val remainingEvents = doVersionWorkingCopyCreated(fxf, copies, events)
    val copy = currentCopyNumber(fxf, cookie).toString

    // TODO: elasticsearch add index routing
    remainingEvents.foreach {
      case Truncated => doDropCopy(fxf, copy, truncate = true)
      case ColumnCreated(colInfo) => doUpdateMapping(fxf, copy, Some(colInfo.systemId.underlying.toString))
      case ColumnRemoved(colInfo) => { /* TODO: remove column */ }
      case LastModifiedChanged(lastModified) => {
        Await.result(
          esc.index(conf.es.index, fxf, Some(copy), "{\"truthUpdate\":\"%s\"}".format(lastModified)),
          conf.escTimeout
        )
      }
      case WorkingCopyDropped => doDropCopy(fxf, copy)
      case DataCopied => { /* TODO: figure out what is required */ }
      case SnapshotDropped(info) => doDropCopy(fxf, copy)
      case WorkingCopyPublished => { /* TODO: pay attention to working copy lifecycle */ }
      case RowDataUpdated(ops) => doBulkUpsert(s"$fxf-$copy", ops)
      case i: Any => throw new UnsupportedOperationException(s"event not supported: '$i'")
    }

    Await.result(esc.index(conf.es.index, fxf, Some(copy),
        "{\"truthVersion\":\"%d\"%s \"truthUpdate\":\"%d\"}".format(newDataVersion, comma, DateTime.now().getMillis)),
      conf.escTimeoutFast
    )

    cookie
  }

  private[this] def doVersionWorkingCopyCreated(fxf: String, copies: List[(Long,Long)],
                                                events: Iterator[Event[SoQLType, SoQLValue]]):
  Iterator[Event[SoQLType, SoQLValue]] = {
    val (wccEvents, remainingEvents) = events.span {
      case WorkingCopyCreated(_) => true
      case _ => false
    }

    // got working copy event
    if (wccEvents.hasNext) {
      val WorkingCopyCreated(copyInfo) = wccEvents.next()
      if (copyInfo.copyNumber < 0) throw new UnsupportedOperationException(s"copy invalid: $copyInfo")
      copies.map {
        case (id, _) => if (copyInfo.copyNumber <= id) {
          throw new UnsupportedOperationException(s"copy already assigned: $id")
        }
      }
      doUpdateMapping(fxf, copyInfo.copyNumber.toString)
    }

    if (wccEvents.hasNext) {
      val msg = s"Got ${wccEvents.size + 1} leading WorkingCopyCreated events$comma only support one in a version"
      throw new UnsupportedOperationException(msg)
    }

    remainingEvents
  }

  // TODO: update mapping if column is not yet mapped with completion analyzer
  private[this] def doBulkUpsert(fxfcopy: String, ops: Seq[Operation[SoQLValue]]) = {
    ops.grouped(bulkBatchSize).foreach {
      batch: Seq[Operation[SoQLValue]] => {
        val bulkPayload = new StringBuilder
        batch.foreach {
          case Insert(id, data) =>
            bulkPayload.append("{\"index\": {\"_id\": %s} }\n%s\n".format(id.underlying, rowToJson(data)))
          case Update(id, data) =>
            bulkPayload.append("{\"update\": {\"_id\": %s} }\n{\"doc\": %s}\n".format(id.underlying,
              rowToJson(data)))
          case Delete(id) => bulkPayload.append("{\"delete\": {\"_id\": %s} }\n".format(id.underlying))
        }
        Await.result(esc.bulk(Some(conf.es.index), Some(fxfcopy), bulkPayload.toString()), conf.escTimeout)
      }

    }
  }

  // TODO use actual dataset key
  val cryptProvider = new CryptProvider("TODO".getBytes)
  val jsonReps = SoQLRep.jsonRep(new SoQLID.StringRep(cryptProvider), new SoQLVersion.StringRep(cryptProvider))

  private[this] def rowToJson(data: Row[SoQLValue]): String = {
    // TODO We need the UserColumnId to actually be able to have soda fountain map, it doesn't have the SystemColumnId.
    data.toSeq.map { case (id: ColumnId, v: SoQLValue) =>
      "\"%s\": %s".format(id.underlying.toString, jsonReps(v.typ).toJValue(v))
    }.mkString("{", comma, "}")
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

    for { iter <- rows } {
      // TODO this multi level batching isn't entirely sane, revisit
      iter.grouped(bulkBatchSize).foreach { bi =>
        val batch = bi.map { row =>
          val docId: Long = row(sysIdCol) match {
            case SoQLID(n) => n
            case x: Any => throw new NumberFormatException(s"Unknown system id column $x")
          }
          Insert(new RowId(docId), row)
        }
        doBulkUpsert(s"$fxf-$copy", batch)
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
    val newMapping = mappingBase.format(fxfcopy, newColumns.map(mappingCol.format(_)).mkString(comma))

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
