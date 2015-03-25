package com.socrata.spandex.common

import com.socrata.spandex.common.client.{FieldValue, SpandexElasticSearchClient}
import com.rojoma.json.v3.util.JsonUtil

trait TestESData {
  case class IndexEntry(id: String, source: String)

  val datasets = Seq("primus.1234", "primus.9876")
  val columns  = Seq("col1-1111", "col2-2222", "col3-3333")

  def config: SpandexConfig
  def client: SpandexElasticSearchClient

  def bootstrapData(): Unit = {
    for {
      ds     <- datasets
      copy   <- 1 to 2
      column <- 1 to 3
      row    <- 1 to 5
    } {
      val doc = FieldValue(ds, copy, column, columns(column - 1), row, "data" + row)
      val response = client.client.prepareIndex(
        config.es.index, config.es.fieldValueMapping.mappingType, doc.docId)
          .setSource(JsonUtil.renderJson(doc))
          .execute.actionGet
      assert(response.isCreated, s"failed to create ${doc.docId}->$row")
    }

    // wait a sec to let elasticsearch index the documents
    Thread.sleep(1000) // scalastyle:ignore magic.number
  }

  def removeBootstrapData(): Unit = {
    datasets.foreach(client.deleteFieldValuesByDataset)
  }
}
