package com.socrata.spandex.common

import org.elasticsearch.rest.RestStatus
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers, FunSuiteLike}

class ElasticSearchClientSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestESData {
  val config = new SpandexConfig
  val client = new TestESClient(config.es)

  override def afterAll(): Unit = client.close()

  override def beforeEach(): Unit = bootstrapData()
  override def afterEach(): Unit = removeBootstrapData()

  test("Delete by dataset") {
    client.searchByDataset(datasets(0)).getHits.totalHits should be (30)
    client.searchByDataset(datasets(1)).getHits.totalHits should be (30)

    val response = client.deleteByDataset(datasets(0))
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchByDataset(datasets(0)).getHits.totalHits should be (0)
    client.searchByDataset(datasets(1)).getHits.totalHits should be (30)
  }

  test("Delete by copy ID") {
    client.searchByCopyId(datasets(0), 1).getHits.totalHits should be (15)
    client.searchByCopyId(datasets(0), 2).getHits.totalHits should be (15)
    client.searchByCopyId(datasets(1), 1).getHits.totalHits should be (15)
    client.searchByCopyId(datasets(1), 2).getHits.totalHits should be (15)

    val response = client.deleteByCopyId(datasets(0), 2)
    response.status() should be (RestStatus.OK)
    response.getIndices.get(config.es.index).getFailures.size should be (0)

    client.searchByCopyId(datasets(0), 1).getHits.totalHits should be (15)
    client.searchByCopyId(datasets(0), 2).getHits.totalHits should be (0)
    client.searchByCopyId(datasets(1), 1).getHits.totalHits should be (15)
    client.searchByCopyId(datasets(1), 2).getHits.totalHits should be (15)
  }
}
