package com.socrata.spandex.common.client

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkResponse}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import ResponseExtensions._

class ResponseExtensionsSpec extends FunSuiteLike
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  test("Create BulkResponseAcknowledgement from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, "create", new IndexResponse("spandex", "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, "create", new IndexResponse("spandex", "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, "create", new IndexResponse("spandex", "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(4, "delete", new DeleteResponse("spandex", "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, "update", new UpdateResponse("spandex", "field_value", "alpha.1234", 1L, false)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)
    val expectedAcknowledgement = BulkResponseAcknowledgement(
      deletions = Map("dataset_copy" -> 1),
      updates = Map("field_value" -> 1),
      creations = Map("dataset_copy" -> 1, "field_value" -> 1, "column_map" -> 1))

    BulkResponseAcknowledgement(bulkResponse) should be (expectedAcknowledgement)
  }

  test("Get non-empty deletions count map from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, "create", new IndexResponse("spandex", "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, "create", new IndexResponse("spandex", "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, "create", new IndexResponse("spandex", "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(4, "delete", new DeleteResponse("spandex", "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, "update", new UpdateResponse("spandex", "field_value", "alpha.1234", 1L, false)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)
    val expectedDeletionsCountMap = Map("dataset_copy" -> 1)

    bulkResponse.deletions should be (expectedDeletionsCountMap)
  }

  test("Get empty deletions count map from BulkResponse") {
    val itemResponses = Array(
      new BulkItemResponse(1, "create", new IndexResponse("spandex", "dataset_copy", "alpha.1234", 0L, true)),
      new BulkItemResponse(2, "create", new IndexResponse("spandex", "field_value", "alpha.1234", 0L, true)),
      new BulkItemResponse(3, "create", new IndexResponse("spandex", "column_map", "alpha.1234", 0L, true)),
      new BulkItemResponse(5, "update", new UpdateResponse("spandex", "field_value", "alpha.1234", 1L, false)))

    val bulkResponse = new BulkResponse(itemResponses, 10L)

    bulkResponse.deletions shouldBe empty
  }
}
