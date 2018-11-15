package com.socrata.spandex.common.client

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import org.scalatest._

class ColumnValueSpec extends FunSuite with Matchers {

  test("test aggregate large values") {

    val wordsBuffer = ListBuffer[String]()
    val columValBuffer = ListBuffer[ColumnValue]()

    val fileStream = getClass.getResourceAsStream("/words.txt")
    for (line <- Source.fromInputStream(fileStream).getLines) {
      wordsBuffer += line.trim()
    }

    val words = wordsBuffer.toArray
    //Seed the random number generator with something predictable so we can make assertions about the results
    val rand = new Random(1)
    val datasetId = "dataset1"

    List(1L, 2L, 3L, 4L, 5L, 6L).foreach { columnId =>
      for (_ <- 0 until 100000) {
        columValBuffer += ColumnValue(columnId = columnId,
          datasetId = datasetId,
          value = words(rand.nextInt(words.length)),
          count = 1L,
          copyNumber = 1L)
      }
    }

    val colIterator = ColumnValue.aggregate(Random.shuffle(columValBuffer.toList))
    val wordMap = colIterator.map(cv => cv.value + cv.columnId -> cv.count).toMap

    wordMap.getOrElse("pharyngoplegy6", 0) should be(3)
    wordMap.getOrElse("auditive5", 0) should be(1)
    wordMap.getOrElse("demiatheist2", 0) should be(3)
    wordMap.getOrElse("defer4", 0) should be(2)
  }

  test("test aggregate correctness") {
    val values = List(
      ColumnValue(datasetId = "1234", columnId = 1, copyNumber = 1, count = 1, value = "Hello"),
      ColumnValue(datasetId = "1234", columnId = 1, copyNumber = 1, count = 1, value = "Hello"),
      ColumnValue(datasetId = "1234", columnId = 2, copyNumber = 1, count = 1, value = "Single Instance"),
      ColumnValue(datasetId = "1234", columnId = 3, copyNumber = 1, count = 1, value = "Column 3"),
      ColumnValue(datasetId = "1234", columnId = 3, copyNumber = 1, count = 1, value = "Column 3"),
      ColumnValue(datasetId = "1234", columnId = 3, copyNumber = 1, count = 1, value = "Column 3"),
      ColumnValue(datasetId = "1234", columnId = 3, copyNumber = 1, count = 1, value = "Single Instance in Col 3"),
      ColumnValue(datasetId = "1234", columnId = 4, copyNumber = 1, count = 1, value = "Column 4"),
      ColumnValue(datasetId = "1234", columnId = 5, copyNumber = 1, count = 1, value = "Another String"),
      ColumnValue(datasetId = "1234", columnId = 5, copyNumber = 1, count = 1, value = "Another String"),
      ColumnValue(datasetId = "1234", columnId = 5, copyNumber = 1, count = 1, value = "Hello"),
      ColumnValue(datasetId = "1234", columnId = 5, copyNumber = 1, count = 1, value = "I expect 2 of these"),
      ColumnValue(datasetId = "1234", columnId = 5, copyNumber = 1, count = 1, value = "I expect 2 of these"))

    val colIterator = ColumnValue.aggregate(Random.shuffle(values))
    val wordMap = colIterator.map(cv => cv.value + cv.columnId -> cv.count).toMap

    wordMap.getOrElse("Hello1", 0) should be(2)
    wordMap.getOrElse("Column 33", 0) should be(3)
    wordMap.getOrElse("Hello5", 0) should be(1)
    wordMap.getOrElse("Single Instance2", 0) should be(1)
    wordMap.getOrElse("Single Instance in Col 33", 0) should be(1)
    wordMap.getOrElse("Column 44", 0) should be(1)
    wordMap.getOrElse("Another String5", 0) should be(2)
    wordMap.getOrElse("I expect 2 of these5", 0) should be(2)
  }
}
