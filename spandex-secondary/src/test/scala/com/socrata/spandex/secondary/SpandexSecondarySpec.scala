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
  val datasetinfo = DatasetInfo(fxf, "en-us", Array.empty)
  val copyinfo0 = CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now)
  val copynum42 = 42
  val copyinfo42 = CopyInfo(new CopyId(copynum42), copynum42, LifecycleStage.Unpublished, 0, DateTime.now)

  private def bootstrapImaginaryData(): Unit = {
    val conf = new SpandexConfig
    val esc = new ElasticsearchClient(conf.esUrl)
    SpandexBootstrap.ensureIndex(conf)
    Await.result(
      esc.index(conf.index, fxf, Some(copyinfo42.copyNumber.toString),
        """
          |{
          | "truthVersion": "42",
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
    val cookie = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(
      new WorkingCopyCreated(copyinfo0)
    ))
  }

  test("snapshots") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val copies = sec.snapshots(fxf, None)
    copies should contain(copyinfo42.copyNumber)
  }

  test("current copy num") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.currentCopyNumber(fxf, None)
  }

  test("wants working copies") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.wantsWorkingCopies
  }

  test("get current version") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.currentVersion(fxf, None)
  }

  test("version") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator.empty)
  }

  test("version with too many working copy event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(
        new WorkingCopyCreated(copyinfo0),
        new WorkingCopyCreated(copyinfo0)
      ))
    }
  }

  test("version with already used data version number") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      val _ = sec.version(datasetinfo, -1, None, Iterator.empty)
    }
  }

  test("version truncate") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(Truncated))
  }

  val col0 = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("col0"), SoQLText,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false)
  test("version column create") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(
      new ColumnCreated(col0)
    ))
  }

  test("version column remove") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(
      new ColumnRemoved(col0)
    ))
  }

  test("version row data updated") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(
      new RowDataUpdated(Seq.empty)
    ))
  }

  test("version data copied") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(DataCopied))
  }

  test("version working copy published") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(WorkingCopyPublished))
  }

  test("version snapshot dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(new SnapshotDropped(
      copyinfo0
    )))
  }

  test("version working copy dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(WorkingCopyDropped))
  }

  test("version unsupported event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      val _ = sec.version(datasetinfo, copyinfo0.copyNumber, None, Iterator(RollupCreatedOrUpdated(
        new RollupInfo("roll", "select *")
      )))
    }
  }

  val colSysid = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("sysid"), SoQLText,
    isSystemPrimaryKey = true, isUserPrimaryKey =  false, isVersion =  false)
  test("resync") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.resync(datasetinfo,
      copyinfo0,
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
    val _ = sec.resync(datasetinfo,
      copyinfo0,
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
