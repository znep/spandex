package com.socrata.spandex.data

import java.io.File
import scala.io.Source
import scala.collection.JavaConverters._
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.datacoordinator.secondary.LifecycleStage

import com.socrata.spandex.common.client.{ColumnMap, ColumnValue, DatasetCopy}
import com.socrata.spandex.common.SpandexIntegrationTest

class LoaderSpec extends FunSuiteLike
  with Matchers
  with PropertyChecks
  with SpandexIntegrationTest {

  val datasetId = "brown_corpus"

  test("The Loader should load the expected data with the correct counts") {
    val loader = new Loader(client)
    val dataFile = new File(getClass.getResource("/brown_tokens.csv").getFile())
    val copyNumber = 1L
    val version = 1L
    val datasetCopy = DatasetCopy(datasetId, copyNumber, version, LifecycleStage.Published)
    val columnMaps = Map(0 -> ColumnMap(datasetId, copyNumber, 0, "token"))

    loader.loadFromPath(dataFile, datasetCopy, columnMaps)
  }

  test("The counts for each token should match the expected counts") {
    val expectedCounts = Source.fromInputStream(
      getClass.getResourceAsStream("/brown_tokens.counts.sorted.tsv")
    ).getLines.collect {
      case line if (line.trim != "") =>
        val parts = line.split('\t')
        (parts(0), parts(1).toInt)
    }.toMap

    expectedCounts.foreach { case (token, expected) =>
      val count = client.fetchCountForColumnValue(datasetId, token)
      count should be(Some(expected))
    }
  }
}
