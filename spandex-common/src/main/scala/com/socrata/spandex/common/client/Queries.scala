package com.socrata.spandex.common.client

import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.query.QueryBuilders._

import com.socrata.datacoordinator.secondary.LifecycleStage

object Queries {
  def byDatasetId(datasetId: String): TermQueryBuilder =
    termQuery(SpandexFields.DatasetId, datasetId)

  def byDatasetIdAndStage(datasetId: String, stage: LifecycleStage): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.Stage, stage.toString))

  def byDatasetIdAndCopyNumber(datasetId: String, copyNumber: Long): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.CopyNumber, copyNumber))

  def byDatasetIdCopyNumberAndColumnId(datasetId: String, copyNumber: Long, columnId: Long): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.DatasetId, datasetId))
      .filter(termQuery(SpandexFields.CopyNumber, copyNumber))
      .filter(termQuery(SpandexFields.ColumnId, columnId))

  def byCompositeId(column: ColumnMap): BoolQueryBuilder =
    boolQuery()
      .filter(termQuery(SpandexFields.CompositeId, column.compositeId))

  def byDatasetIdAndOptionalStage(datasetId: String, stage: Option[Stage]): QueryBuilder =
    stage match {
      case Some(n @ Number(_)) => throw new IllegalArgumentException(s"cannot request latest copy for stage = $n")
      case Some(Unpublished) => byDatasetIdAndStage(datasetId, LifecycleStage.Unpublished)
      case Some(Published) => byDatasetIdAndStage(datasetId, LifecycleStage.Published)
      case Some(Snapshotted) => byDatasetIdAndStage(datasetId, LifecycleStage.Snapshotted)
      case Some(Discarded) => byDatasetIdAndStage(datasetId, LifecycleStage.Discarded)
      case _ => byDatasetId(datasetId)
    }

  val AutocompleteMinimumShouldMatch = "100%"

  def byColumnValueAutocomplete(columnValue: Option[String]): QueryBuilder =
    columnValue match {
      case Some(value) =>
        matchQuery(SpandexFields.ValueAutocomplete, value).minimumShouldMatch(AutocompleteMinimumShouldMatch)
      case None => matchAllQuery()
    }

  def byColumnValueAutocompleteAndCompositeId(columnValue: Option[String], column: ColumnMap): BoolQueryBuilder =
    boolQuery()
      .filter(byCompositeId(column))
      .must(byColumnValueAutocomplete(columnValue))

  def nonPositiveCountColumnValuesByDatasetIdAndCopyNumber(datasetId: String, copyNumber: Long): BoolQueryBuilder =
    byDatasetIdAndCopyNumber(datasetId, copyNumber).filter(rangeQuery(SpandexFields.Count).lte(0))
}
