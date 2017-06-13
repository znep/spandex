package com.socrata.spandex.common

import java.util.regex.Pattern
import scala.util.Try

import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.pattern.PatternTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenFilter, TokenStream}
import org.apache.lucene.util.Version

import com.socrata.spandex.common.CompletionAnalyzer.TokenStreamExtensions
import com.socrata.spandex.common.client.SpandexFields

// Elasticsearch mapping type 'completion' is limited to keyword prefix matches.
// Completions of term matches mid-phrase are not natively supported.
// Phrase and Term suggesters, relying on full query, are capable.
// But by specifying an array of matching 'input' values we can trick Lucene
// into prefixing on any term in the 'output' value, the original phrase.
object CompletionAnalyzer {
  private[this] var analyzer: Option[CompletionAnalyzer] = None

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
  // In Lucene Pattern Tokenizer: delimiters separate tokens, and then are removed from indexing and analysis.
  //
  // The code points in this pattern are delimiters and all others are tokenizable.
  // Delimiters include p{C} control characters, p{P} punctuation, p{S*} symbols (except currency),
  // and p{Z} separators, but specifically exclude ampersand.
  // That leaves tokenizable characters as p{L} letters from any language, p{M} combining marks,
  // p{Sc} currency symbols, p{N} numbers, and by exclusion ampersand.
  //
  // This is better than the W nonword character class which defines tokenizable as a-zA-Z_0-9
  // and all others, including non latin character set, as delimiters.
  val analyzer = new PatternAnalyzer(version, Pattern.compile(
    """[\p{C}\p{P}\p{Sm}\p{Sk}\p{So}\p{Z}&&[^\&]]+"""))

  // this takes in the original string (as in a dataset cell), and returns a list of strings
  // that we want to feed to the analyzer as input matches; individual stages described below
  // with each function, but here's the summary of in-out:
  // cares about two config values: config.analysis.{maxInputLength, maxShingleLength}
  // in: "The puppies ran around the room. The kittens chased after them. Everyone laughed!"
  // (luceneized): "the puppies ran around the room the kittens chased after them everyone laughed"
  //               |------------ maxInputLength ignoring whitespace -----------|
  // and then for reference below:
  //           |---------- maxShingleLength ---------|  (also ignoring whitespace!)
  // out: List("the puppies ran around the room the kittens",
  //           "puppies ran around the room the kittens",
  //           "ran around the room the kittens chased",
  //           ...
  //           "kittens chased after them",
  //           "chased after them",
  //           "after them",
  //           "them")
  // this list is then fed to elasticsearch as matchable options
  def analyze(value: String): List[String] = { // scalastyle:ignore method.length
    // goes through the luceneized original string (accessible via TokenStream)
    // and adds the tokens to a list (prepending, so eventually in reverse order)
    // up until the combined length of the tokens (without whitespace!) is maxLength (or less)
    // or we run out of tokens. will only add whole tokens to the list, not partial
    // so input TokenStream = [A B C D ...] where len(A B C) <= maxLength and len(A B C D) > maxLength
    // and output will be [C B A]
    @annotation.tailrec
    def tokenize(stream: TokenStream, maxLength: Int, acc: List[String]): List[String] = {
      // if the next token will take us over maxLength, stop and return the accumulated list
      if (stream.incrementToken() && maxLength > 0) {
        val token = stream.getAttribute(classOf[CharTermAttribute]).toString
        tokenize(stream, maxLength - token.length , token :: acc)
      } else { acc }
    }

    // this starts with the (possibly shortened) token list of the original string
    // and then shingles over that list + all the lists formed by popping off the first token recursively
    // so token input: [A B C D E]
    // winds up shingling over [A B C D E], [B C D E], [C D E], [D E], [E], []
    @annotation.tailrec
    def foldConcat(tokens: List[String], acc: List[String]): List[String] = {
      tokens match {
        case Nil => acc
        case h :: t =>
          foldConcat(t, shingle(h :: t, config.maxShingleLength, Nil).reverse.mkString(" ") :: acc)
      }
    }

    // a shingle is effectively a substring composed of n tokens of the original string
    // this starts with a list of tokens and will prepend each token to a list
    // up until we have run out of tokens or have already gone past maxLength
    // (so if maxLength falls in the middle of the current token, it is still included)
    // so token input: [A B C D] where len(A + B) < maxLength and len(A + B + C) >= maxLength
    // yields: [C B A] (because prepending to the list)
    @annotation.tailrec
    def shingle(tokens: List[String], maxLength: Int, acc: List[String]): List[String] = {
      tokens match {
        case Nil => acc
        case h :: t if maxLength > 0 => shingle(t, maxLength - h.length, h :: acc)
        case _ => acc
      }
    }

    // takes the original string and gets luceneish tokens out of it
    // which largely means things like punctuation will be dropped (see longer comment above)
    val stream: TokenStream = analyzer.tokenStream(SpandexFields.Suggest, value)
      .filterLowerCase
    stream.reset()
    // get the portion of the TokenStream that we'll actually get substrings of and allow searching against
    // this works out to the portion of the luceneized tokens that goes out to maxInputLength, rounding down
    val tokens = tokenize(stream, config.maxInputLength, Nil).reverse
    stream.close()

    // get the actual token n-grams we'll allow completion matching against
    foldConcat(tokens, Nil)
  }
}

// Lucene's miscellaneous PatternAnalyzer is deprecated in 4.2.0
protected class PatternAnalyzer(version: Version, pattern: Pattern) extends Analyzer {
  override def createComponents(fieldName: String): TokenStreamComponents = {
    val source = new TokenStreamComponents(new PatternTokenizer(pattern, -1))
    val result = new LowerCaseFilter(source.getTokenStream)
    new TokenStreamComponents(source.getTokenizer, result)
  }
}
