package com.socrata.spandex.secondary

import com.socrata.datacoordinator.id.{ColumnId, RowId}
import com.socrata.soql.types.{SoQLValue, SoQLText}
import com.socrata.spandex.common.client.{SpandexElasticSearchClient, FieldValue}
import com.socrata.datacoordinator.secondary._
import com.typesafe.scalalogging.slf4j.Logging
import org.elasticsearch.action.ActionRequestBuilder
import RowOpsHandler._

case class RowOpsHandler(client: SpandexElasticSearchClient, batchSize: Int) extends Logging {
  type RequestBuilder = ActionRequestBuilder[_,_,_,_]

  private def requestsForRow(datasetName: String,
                             copyNumber: Long,
                             rowId: RowId,
                             data: Row[SoQLValue],
                             builder: FieldValue => RequestBuilder): Seq[RequestBuilder] = {
    data.toSeq.collect {
      // Spandex only stores text columns; other column types are a no op
      case (id, value: SoQLText) =>
        builder(fieldValueFromDatum(datasetName, copyNumber, rowId, (id, value)))
    }
  }

  def go(datasetName: String, copyNumber: Long, ops: Seq[Operation]): Unit = {
    val columnIds = client.searchColumnMapsByCopyNumber(datasetName, copyNumber)
                          .thisPage.map(_.systemColumnId)

    val requests: Seq[RequestBuilder] = ops.flatMap { op =>
      logger.debug("Received row operation: " + op)
      op match {
        case Insert(rowId, data) =>
          requestsForRow(datasetName, copyNumber, rowId, data, client.getFieldValueIndexRequest)
        case Update(rowId, data) =>
          requestsForRow(datasetName, copyNumber, rowId, data, client.getFieldValueUpdateRequest)
        case Delete(rowId)       =>
          columnIds.map { colId =>
            client.getFieldValueDeleteRequest(datasetName, copyNumber, colId, rowId.underlying)
          }
      }
    }

    // TODO : Guarantee refresh before read instead of after write
    for { batch <- requests.grouped(batchSize) } {
      client.sendBulkRequest(batch, refresh = true)
    }
  }
}

object RowOpsHandler {
  def fieldValueFromDatum(datasetName: String,
                          copyNumber: Long,
                          rowId: RowId,
                          datum: (ColumnId, SoQLText)): FieldValue = datum match {
    case (id, value) => FieldValue(datasetName, copyNumber, id.underlying, rowId.underlying, value.value)
  }
}
