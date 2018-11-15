package com.socrata.spandex.data

import java.io.File
import scala.io.Source
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.datacoordinator.secondary.LifecycleStage

import com.socrata.spandex.common.client.{ColumnMap, ColumnValue, DatasetCopy}
import com.socrata.spandex.common.SpandexIntegrationTest

class LoaderSpec extends FunSuiteLike
  with Matchers
  with PropertyChecks
  with SpandexIntegrationTest {

  val datasetId1 = "brown_corpus1"
  val datasetId2 = "brown_corpus2"

  test("The Loader should load the expected data with the correct counts") {
    val loader = new Loader(client)
    val dataFile = new File(getClass.getResource("/brown_tokens.csv").getFile())
    val copyNumber = 1L
    val version = 1L

    val loaderThread1 = new Thread(){
      override def run: Unit ={
        val datasetCopy = DatasetCopy(datasetId1, copyNumber, version, LifecycleStage.Published)
        val columnMaps = Map(0 -> ColumnMap(datasetId1, copyNumber, 0, "token"))
        loader.loadFromPath(dataFile, datasetCopy, columnMaps)
      }
    }

    val loaderThread2 = new Thread(){
      override def run: Unit = {
        val datasetCopy = DatasetCopy(datasetId2, copyNumber, version, LifecycleStage.Published)
        val columnMaps = Map(0 -> ColumnMap(datasetId2, copyNumber, 0, "token"))
        loader.loadFromPath(dataFile, datasetCopy, columnMaps)
      }
    }

    List(loaderThread1, loaderThread2).foreach(_.start())
    List(loaderThread1, loaderThread2).foreach(_.join)
  }

  test("The counts for each token should match the expected counts, " +
    "even when two documents are loaded at once") {
    val expectedCounts = Source.fromInputStream(
      getClass.getResourceAsStream("/brown_tokens.counts.sorted.tsv")
    ).getLines.collect {
      case line if (line.trim != "") =>
        val parts = line.split('\t')
        (parts(0), parts(1).toInt)
    }.toMap

    List(datasetId1, datasetId2).foreach(datasetId =>
      expectedCounts.foreach { case (token, expected) =>
        val count = client.fetchCountForColumnValue(datasetId, token)
        count should be(Some(expected))
      }
    )
  }
}
