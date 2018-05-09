package com.socrata.spandex.common

import scala.collection.JavaConverters._

import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.spandex.common.client._

case class Token(text: String, offset: Int)

object Token {
  def apply(token: AnalyzeToken): Token =
    Token(token.getTerm, token.getStartOffset)
}

class AnalyzersSpec extends WordSpec
  with ShouldMatchers
  with BeforeAndAfterAll
  with SpandexIntegrationTest {

  "the autocomplete analyzer should preserve the surface form and accent-normalized " +
  "versions of a token with accented characters" in {
    val expectedTokens = List(
      Token("s", 0), Token("sy", 0), Token("sys", 0), Token("syst", 0),
      Token("systé", 0), Token("systém", 0), Token("syste", 0), Token("system", 0),
      Token("systéma", 0), Token("systema", 0), Token("systémat", 0),
      Token("systemat", 0), Token("systémati", 0), Token("systemati", 0),
      Token("systématiq", 0), Token("systematiq", 0), Token("systématiqu", 0),
      Token("systematiqu", 0), Token("systématique", 0), Token("systematique", 0))

    val actualTokens = client.analyze("autocomplete", "systématique")
    actualTokens should contain allOf (Token("systématique", 0), Token("systematique", 0))
    actualTokens should contain theSameElementsAs(expectedTokens)
  }

  "the case_insensitive_word query analyzer should not normalize accented characters" in {
    client.analyze("case_insensitive_word", "systématique") should contain (Token("systématique", 0))
  }
}
