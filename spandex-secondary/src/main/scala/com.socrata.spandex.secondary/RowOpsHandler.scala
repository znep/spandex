package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.RowId
import com.socrata.soql.types.{SoQLValue, SoQLText}
import com.socrata.spandex.common.client.{SpandexElasticSearchClient, FieldValue}
import com.socrata.datacoordinator.secondary._
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.ActionRequestBuilder

case class RowOpsHandler(client: SpandexElasticSearchClient) extends Logging {
  private def handleOp(datasetName: String,
               copyNumber: Long,
               rowId: RowId,
               data: Row[SoQLValue],
               f: FieldValue => ActionRequestBuilder[_,_,_,_]): Unit = {
    val requests = data.toSeq.collect {
      // Spandex only stores text columns; other column types are a no op
      case (id, value: SoQLText) =>
        f(FieldValue(datasetName, copyNumber, id, rowId, value))
    }
    client.sendBulkRequest(requests)
  }

  def go(datasetName: String, copyNumber: Long, ops: Seq[Operation]): Unit = ops.foreach { op =>
    // Boo, hiss, we cannot use a bulk request for the whole lot,
    // because ES does not support DeleteByQuery in a bulk request,
    // and we have to maintain order of operations.
    // We will bulk up inserts and updates on the same row though.
    logger.debug("Received row operation: " + op)
    op match {
      case Insert(rowId, data) =>
        handleOp(datasetName, copyNumber, rowId, data, client.getIndexRequest)
      case Update(rowId, data) =>
        handleOp(datasetName, copyNumber, rowId, data, client.getUpdateRequest)
      case Delete(rowId)       =>
        client.deleteFieldValuesByRowId(datasetName, copyNumber, rowId.underlying)
    }
  }
}
