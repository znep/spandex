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

  private[this] def requestsForRow(datasetName: String,
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
    // If there are deletes, we need the dataset's column IDs. If not, save the call to ES.
    val columnIds =
      if (ops.exists(_.isInstanceOf[Delete])) {
      client.searchLotsOfColumnMapsByCopyNumber(datasetName, copyNumber)
        .thisPage.map(_.systemColumnId)
      } else {
        Seq.empty
      }

    val requests: Seq[RequestBuilder] = ops.flatMap { op =>
      logger.debug("Received row operation: " + op)
      op match {
        case Insert(rowId, data) =>
          requestsForRow(datasetName, copyNumber, rowId, data, client.fieldValueIndexRequest)
        case Update(rowId, data) =>
          requestsForRow(datasetName, copyNumber, rowId, data, client.fieldValueUpdateRequest)
        case Delete(rowId)       =>
          columnIds.map { colId =>
            client.fieldValueDeleteRequest(datasetName, copyNumber, colId, rowId.underlying)
          }
        case _ => throw new UnsupportedOperationException(s"Row operation ${op.getClass.getSimpleName} not supported")
      }
    }

    // The requests will be issued in order, so we don't need to
    // refresh after every batch, only afterwards.
    // TODO : Guarantee refresh before read instead of after write
    for { batch <- requests.grouped(batchSize) } {
      client.sendBulkRequest(batch, refresh = false)
    }

    client.refresh()
  }
}

object RowOpsHandler {
  def fieldValueFromDatum(datasetName: String,
                          copyNumber: Long,
                          rowId: RowId,
                          datum: (ColumnId, SoQLText)): FieldValue = datum match {
    case (id, value) => FieldValue(datasetName, copyNumber, id.underlying, rowId.underlying,
      // *sigh* a cluster side analysis char_filter doesn't catch this one character in time.
      value.value.replaceAll("\u001f", ""))
  }
}
