package com.socrata.spandex.secondary

import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.id.{ColumnId, CopyId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common._
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import wabisabi.{Client => ElasticsearchClient}

import scala.concurrent.Await

class SpandexSecondarySpec extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  val fxf = "qnmj-8ku6"
  val dataSetInfo = DatasetInfo(fxf, "en-us", Array.empty)
  val copyNum42 = 42
  private[this] var _copy = copyNum42
  def copy = {
    _copy = _copy + 1
    _copy
  }
  val versionNum1234 = 1234
  private[this] var _version = versionNum1234
  def version = {
    _version = _version + 1
    _version
  }
  def copyInfo(id: Long = copy, ver: Long = version) =
    CopyInfo(new CopyId(id), id, LifecycleStage.Unpublished, ver, DateTime.now)

  private[this] def bootstrapImaginaryData(): Unit = {
    val conf = new SpandexConfig
    val esc = new ElasticsearchClient(conf.esUrl)
    SpandexBootstrap.ensureIndex(conf)
    Await.result(
      esc.index(conf.index, fxf, Some(copyNum42.toString),
        s"""
          |{
          | "truthVersion": "$version",
          | "truthUpdate": "1234567890"
          |}
        """.stripMargin), conf.escTimeoutFast)
    // wait a sec to let elasticsearch index the document
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    bootstrapImaginaryData()
  }

  test("ctor") {
    val sec = new SpandexSecondary(new SpandexConfig)
  }

  test("add dataset: version with working copy event") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val cookie = sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
      new WorkingCopyCreated(copyInfo())
    ))
  }

  test("snapshots") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val copies = sec.snapshots(fxf, None)
    copies should contain(copyNum42)
  }

  test("current copy num") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.currentCopyNumber(fxf, None)
  }

  test("wants working copies") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.wantsWorkingCopies
  }

  test("get current version") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.currentVersion(fxf, None)
  }

  test("version") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, version, None, Iterator.empty)
  }

  test("version with too many working copy event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
        new WorkingCopyCreated(copyInfo()),
        new WorkingCopyCreated(copyInfo())
      ))
    }
  }

  test("version with invalid data copy number") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, version, None, Iterator(
        WorkingCopyCreated(copyInfo(id = -1))
      ))
    }
  }

  test("version with already used copy number") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
        new WorkingCopyCreated(copyInfo(id = copyNum42))
      ))
    }
  }

  test("version with invalid data version number") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, -1, None, Iterator.empty)
    }
  }

  test("version with already used version number") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, versionNum1234, None, Iterator(
        new WorkingCopyCreated(copyInfo())
      ))
    }
  }

  test("version truncate") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(Truncated))
  }

  val col0 = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("col0"), SoQLText,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false)
  test("version column create") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
      new ColumnCreated(col0)
    ))
  }

  test("version column remove") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
      new ColumnRemoved(col0)
    ))
  }

  test("version row data updated") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
      new RowDataUpdated(Seq.empty)
    ))
  }

  test("version data copied") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(DataCopied))
  }

  test("version working copy published") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(WorkingCopyPublished))
  }

  test("version snapshot dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(
      new SnapshotDropped(copyInfo())
    ))
  }

  test("version working copy dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(WorkingCopyDropped))
  }

  test("version unsupported event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      sec.version(dataSetInfo, copyInfo().dataVersion, None, Iterator(RollupCreatedOrUpdated(
        new RollupInfo("roll", "select *")
      )))
    }
  }

  val colSysid = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("sysid"), SoQLText,
    isSystemPrimaryKey = true, isUserPrimaryKey =  false, isVersion =  false)
  test("resync") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.resync(dataSetInfo,
      copyInfo(),
      ColumnIdMap((new ColumnId(0), colSysid)),
      None,
      new SimpleArm[Iterator[ColumnIdMap[SoQLValue]]] {
        def flatMap[A](f: Iterator[ColumnIdMap[SoQLValue]] => A): A = { f(Iterator.empty) }
      },
      Seq.empty
    )
  }

  test("resync with data") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.resync(dataSetInfo,
      copyInfo(),
      ColumnIdMap((new ColumnId(0), colSysid)),
      None,
      new SimpleArm[Iterator[ColumnIdMap[SoQLValue]]] {
        def flatMap[A](f: Iterator[ColumnIdMap[SoQLValue]] => A): A = { f(Iterator(
          ColumnIdMap((new ColumnId(0), new SoQLText("this is a test")))
        )) }
      },
      Seq.empty
    )
  }

  test("drop dataset") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.dropDataset(fxf, None)
  }

  test("shutdown") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.shutdown()
  }
}
