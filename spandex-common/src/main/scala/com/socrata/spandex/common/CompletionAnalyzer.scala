package com.socrata.spandex.common

import java.io.Reader
import java.util.regex.Pattern

import com.socrata.spandex.common.CompletionAnalyzer.TokenStreamExtensions
import com.socrata.spandex.common.client.SpandexFields
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.pattern.PatternTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenFilter, TokenStream}
import org.apache.lucene.util.Version

import scala.util.Try

// Elasticsearch mapping type 'completion' is limited to keyword prefix matches.
// Completions of term matches mid-phrase are not natively supported.
// Phrase and Term suggesters, relying on full query, are capable.
// But by specifying an array of matching 'input' values we can trick Lucene
// into prefixing on any term in the 'output' value, the original phrase.
object CompletionAnalyzer {
  private var analyzer: Option[CompletionAnalyzer] = None

  def analyze(value: String): List[String] = analyzer.map(_.analyze(value)).getOrElse(value :: Nil)

  def configure(config: AnalysisConfig): Unit = {
    if (config.enabled) {
      analyzer = Some(new CompletionAnalyzer(config))
    } else {
      analyzer = None
    }
  }

  implicit class TokenStreamExtensions(val tokens: TokenStream) extends AnyVal {
    def filter[T <: TokenFilter](filter: Class[T]): TokenStream =
      filter.getConstructor(classOf[TokenStream])
        .newInstance(tokens)
        .asInstanceOf[TokenStream]

    def filterLowerCase: TokenStream = new LowerCaseFilter(tokens)
  }

}

class CompletionAnalyzer(val config: AnalysisConfig) {
  val version = Try { Version.parseLeniently(config.luceneVersion) }.getOrElse(Version.LATEST)
  val analyzer = new PatternAnalyzer(version, Pattern.compile( """\W+"""))

  def analyze(value: String): List[String] = {
    @annotation.tailrec
    def tokenize(stream: TokenStream, acc: List[String]): List[String] = {
      if (stream.incrementToken()) {
        tokenize(stream, stream.getAttribute(classOf[CharTermAttribute]).toString :: acc)
      } else { acc }
    }

    @annotation.tailrec
    def foldConcat(tokens: List[String], acc: List[String]): List[String] = {
      tokens match {
        case Nil => acc
        case h :: t =>
          val s = (h :: t).mkString(" ")
          foldConcat(t, (if (s.length > config.maxShingleLength) s.substring(0, config.maxShingleLength) else s) :: acc)
      }
    }

    val shortenedValue =
      if (value.length > config.maxInputLength) value.substring(0, config.maxInputLength) else value

    val stream: TokenStream = analyzer.tokenStream(SpandexFields.Value, shortenedValue)
      .filterLowerCase
    stream.reset()
    val tokens = tokenize(stream, Nil).reverse
    stream.close()

    foldConcat(tokens, Nil)
  }
}

// Lucene's miscellaneous PatternAnalyzer is deprecated in 4.2.0
protected class PatternAnalyzer(version: Version, pattern: Pattern) extends Analyzer {
  override def createComponents(fieldName: String, reader: Reader): TokenStreamComponents = {
    val source = new TokenStreamComponents(new PatternTokenizer(reader, pattern, -1))
    val result = new LowerCaseFilter(source.getTokenStream)
    new TokenStreamComponents(source.getTokenizer, result)
  }
}
