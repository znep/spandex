package com.socrata.spandex.common.client

import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders._

import com.socrata.datacoordinator.secondary.LifecycleStage

object Queries {
  def byDatasetIdQuery(datasetId: String): QueryBuilder = termQuery(SpandexFields.DatasetId, datasetId)

  def byDatasetIdAndStageQuery(datasetId: String, stage: LifecycleStage): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.Stage, stage.toString))

  def byCopyNumberQuery(datasetId: String, copyNumber: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))

  def byColumnIdQuery(datasetId: String, copyNumber: Long, columnId: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))
      .must(termQuery(SpandexFields.ColumnId, columnId))

  def byRowIdQuery(datasetId: String, copyNumber: Long, rowId: Long): QueryBuilder =
    boolQuery()
      .must(termQuery(SpandexFields.DatasetId, datasetId))
      .must(termQuery(SpandexFields.CopyNumber, copyNumber))
      .must(termQuery(SpandexFields.RowId, rowId))

  def datasetIdAndOptionalStageQuery(datasetId: String, stage: Option[Stage]): QueryBuilder =
    stage match {
      case Some(n @ Number(_)) => throw new IllegalArgumentException(s"cannot request latest copy for stage = $n")
      case Some(Unpublished) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Unpublished)
      case Some(Published) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Published)
      case Some(Snapshotted) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Snapshotted)
      case Some(Discarded) => byDatasetIdAndStageQuery(datasetId, LifecycleStage.Discarded)
      case _ => byDatasetIdQuery(datasetId)
    }
}
