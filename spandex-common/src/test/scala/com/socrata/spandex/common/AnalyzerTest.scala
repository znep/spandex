package com.socrata.spandex.common

import java.io.File

import com.socrata.datacoordinator.secondary.LifecycleStage
import com.socrata.spandex.common.client.{ColumnMap, DatasetCopy, FieldValue, TestESClient}
import com.typesafe.config.ConfigFactory
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.search.suggest.Suggest.Suggestion
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry

import scala.collection.JavaConverters._
import scala.collection.mutable

trait AnalyzerTest {
  val analyzerEnabled: Boolean

  protected val baseConfig = ConfigFactory.load().getConfig("com.socrata.spandex")
  protected val fieldValueMappingPath = "elastic-search.mappings.field-value.mapping-properties"
  protected val configAnalyzerOff = new SpandexConfig(
    ConfigFactory.parseFile(new File("./spandex-common/src/test/resources/analysisOff.conf"))
      .getConfig("com.socrata.spandex")
      .withFallback(baseConfig)
  )
  protected val configAnalyzerOn = new SpandexConfig(
    ConfigFactory.parseFile(new File("./spandex-common/src/test/resources/analysisOn.conf"))
      .getConfig("com.socrata.spandex")
      .withFallback(baseConfig)
  )

  protected lazy val config = if (analyzerEnabled) configAnalyzerOn else configAnalyzerOff
  protected lazy val client = new TestESClient(config.es)

  protected def analyzerBeforeAll(): Unit = {
    SpandexBootstrap.ensureIndex(config.es, client)
    CompletionAnalyzer.configure(config.analysis)
  }

  protected def analyzerAfterAll(): Unit = {
    client.deleteIndex()
    client.close()
  }

  protected val ds = "ds.one"
  protected val copy = DatasetCopy(ds, 1, 42, LifecycleStage.Published)
  protected val col = ColumnMap(copy.datasetId, copy.copyNumber, 2, "column2")

  protected var docId = 10
  protected def index(value: String): Unit =
    index(FieldValue(col.datasetId, col.copyNumber, col.systemColumnId, docId, value))
  protected def index(fv: FieldValue): Unit = {
    client.indexFieldValue(fv, refresh = true)
    docId += 1
  }

  protected def suggest(query: String, fuzz: Fuzziness = Fuzziness.ZERO): mutable.Buffer[String] = {
    val response = client.suggest(col, 10, query, fuzz, 2, 1)
    val suggest = response.getSuggestion[Suggestion[Entry]]("suggest")
    val entries = suggest.getEntries
    val options = entries.get(0).getOptions
    options.asScala.map(_.getText.toString)
  }
}
