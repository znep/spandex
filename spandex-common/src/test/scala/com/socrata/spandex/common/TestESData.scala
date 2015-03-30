package com.socrata.spandex.common

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")

  def copies(dataset: String): Seq[DatasetCopy] = {
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
        val response = client.client.prepareIndex(
          config.es.index, config.es.datasetCopyMapping.mappingType, copy.docId)
          .setSource(JsonUtil.renderJson(copy))
          .execute.actionGet
        assert(response.isCreated, s"failed to create dataset copy doc ${copy.docId}")

        for {column <- 1 to 3} {
          val col = ColumnMap(ds, copy.copyNumber, column, "col" + column)
          val response = client.client.prepareIndex(
            config.es.index, config.es.columnMapMapping.mappingType, col.docId)
            .setSource(JsonUtil.renderJson(col))
            .execute.actionGet
          assert(response.isCreated, s"failed to create column map ${col.docId}")

          for {row <- 1 to 5} {
            def makeData(col: Int, row: Int): String = s"data column $column row $row"
            val doc = FieldValue(ds, copy.copyNumber, column, row, makeData(column, row))
            val response = client.client.prepareIndex(
              config.es.index, config.es.fieldValueMapping.mappingType, doc.docId)
              .setSource(JsonUtil.renderJson(doc))
              .execute.actionGet
            assert(response.isCreated, s"failed to create ${doc.docId}")
          }
        }
      }
    }

    // wait a sec to let elasticsearch index the documents
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteFieldValuesByDataset)
    datasets.foreach(client.deleteColumnMapsByDataset)
    datasets.foreach(client.deleteDatasetCopiesByDataset)
  }
}
