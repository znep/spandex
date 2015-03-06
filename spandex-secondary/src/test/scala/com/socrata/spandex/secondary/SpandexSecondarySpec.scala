package com.socrata.spandex.secondary

import com.rojoma.simplearm.SimpleArm
import com.socrata.datacoordinator.id.{ColumnId, CopyId, UserColumnId}
import com.socrata.datacoordinator.secondary._
import com.socrata.datacoordinator.util.collection.ColumnIdMap
import com.socrata.soql.types.{SoQLText, SoQLType, SoQLValue}
import com.socrata.spandex.common._
import org.joda.time.DateTime
import org.scalatest.{FunSuiteLike, Matchers}

class SpandexSecondarySpec extends FunSuiteLike with Matchers {
  val fxf = "qnmj-8ku6"
  val datasetinfo = new DatasetInfo(fxf, "en-us", Array.empty)

  test("ctor") {
    val sec = new SpandexSecondary(new SpandexConfig)
  }

  test("shutdown") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.shutdown()
  }

  test("drop dataset") {
    val sec = new SpandexSecondary(new SpandexConfig)
    sec.dropDataset(fxf, None)
  }

  test("snapshots") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.snapshots(fxf, None)
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

  test("version with working copy event") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(
      new WorkingCopyCreated(
        new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now)
      )
    ))
  }

  test("version with too many working copy event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      val _ = sec.version(datasetinfo, 0, None, Iterator(
        new WorkingCopyCreated(
          new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now)
        ),
        new WorkingCopyCreated(
          new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now)
        )
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
    val _ = sec.version(datasetinfo, 0, None, Iterator(Truncated))
  }

  val col0 = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("col0"), SoQLText,
    isSystemPrimaryKey = false, isUserPrimaryKey = false, isVersion = false)
  test("version column create") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(
      new ColumnCreated(col0)
    ))
  }

  test("version column remove") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(
      new ColumnRemoved(col0)
    ))
  }

  test("version row data updated") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(
      new RowDataUpdated(Seq.empty)
    ))
  }

  test("version data copied") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(DataCopied))
  }

  test("version working copy published") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(WorkingCopyPublished))
  }

  test("version snapshot dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(new SnapshotDropped(
      new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now)
    )))
  }

  test("version working copy dropped") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.version(datasetinfo, 0, None, Iterator(WorkingCopyDropped))
  }

  test("version unsupported event throws") {
    a[UnsupportedOperationException] should be thrownBy {
      val sec = new SpandexSecondary(new SpandexConfig)
      val _ = sec.version(datasetinfo, 0, None, Iterator(RollupCreatedOrUpdated(new RollupInfo("roll", "select *"))))
    }
  }

  val colSysid = ColumnInfo[SoQLType](new ColumnId(0), new UserColumnId("sysid"), SoQLText,
    isSystemPrimaryKey = true, isUserPrimaryKey =  false, isVersion =  false)
  test("resync") {
    val sec = new SpandexSecondary(new SpandexConfig)
    val _ = sec.resync(datasetinfo,
      new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now),
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
      new CopyInfo(new CopyId(0), 0, LifecycleStage.Unpublished, 0, DateTime.now),
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
}
