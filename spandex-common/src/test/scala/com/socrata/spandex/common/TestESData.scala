package com.socrata.spandex.common

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")

  def copies(dataset: String) = {
    val snapshot    = DatasetCopy(dataset, 1, 5, LifecycleStage.Snapshotted) // scalastyle:ignore magic.number
    val published   = DatasetCopy(dataset, 2, 10, LifecycleStage.Published) // scalastyle:ignore magic.number
    val workingCopy = DatasetCopy(dataset, 3, 15, LifecycleStage.Unpublished) // scalastyle:ignore magic.number
    Seq(snapshot, published, workingCopy).sortBy(_.copyNumber)
  }

  def config: SpandexConfig
  def client: SpandexElasticSearchClient

  def bootstrapData(): Unit = {
    // Create 2 datasets with 3 copies each:
    // - an old snapshot copy
    // - the most recent published copy
    // - a working copy
    for {
      ds <- datasets
    } {
      for { copy <- copies(ds) } {
        client.putDatasetCopy(ds, copy.copyNumber, copy.version, copy.stage)

        for {column <- 1 to 3} {
          val col = ColumnMap(ds, copy.copyNumber, column, "col" + column)
          client.putColumnMap(ColumnMap(ds, copy.copyNumber, col.systemColumnId, col.userColumnId))

          for {row <- 1 to 5} {
            def makeData(col: Int, row: Int): String = s"data column $column row $row"
            val doc = FieldValue(ds, copy.copyNumber, column, row, makeData(column, row))
            client.indexFieldValue(doc)
          }
        }
      }
    }
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteFieldValuesByDataset)
    datasets.foreach(client.deleteColumnMapsByDataset)
    datasets.foreach(client.deleteDatasetCopiesByDataset)
  }
}
