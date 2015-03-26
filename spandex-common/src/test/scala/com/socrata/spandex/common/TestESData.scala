package com.socrata.spandex.common

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client._

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")

  def config: SpandexConfig
  def client: SpandexElasticSearchClient

  def bootstrapData(): Unit = {
    for {
      ds <- datasets
      copy <- 1 to 2
    } {
      val dsc = DatasetCopy(ds, copy, 0, LifecycleStage.Unpublished)
      val response = client.client.prepareIndex(
        config.es.index, config.es.datasetCopyMapping.mappingType)
        .setSource(JsonUtil.renderJson(dsc))
        .execute.actionGet
      assert(response.isCreated, s"failed to create dataset copy $dsc->$copy")

      for {column <- 1 to 3} {
        val col = ColumnMap(ds, copy, column, "col" + column)
        val response = client.client.prepareIndex(
          config.es.index, config.es.columnMapMapping.mappingType, col.docId)
          .setSource(JsonUtil.renderJson(col))
          .execute.actionGet
        // TODO: columns unique to dataset
        // assert(response.isCreated, s"failed to create column mapping ${col.docId}->$column")

        for {row <- 1 to 5} {
          val doc = FieldValue(ds, copy, column, row, "data" + row)
          val response = client.client.prepareIndex(
            config.es.index, config.es.fieldValueMapping.mappingType, doc.docId)
            .setSource(JsonUtil.renderJson(doc))
            .execute.actionGet
          assert(response.isCreated, s"failed to create ${doc.docId}->$row")
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
