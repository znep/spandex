package com.socrata.spandex.common

import java.io.File

import com.typesafe.config.{ConfigFactory, Config}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class CompletionAnalyzerDietSpec extends FunSuiteLike with Matchers with AnalyzerTest with BeforeAndAfterAll {
  override def testConfig: Config = ConfigFactory.parseFile(new File("./src/test/resources/analysisDiet.conf"))
  override protected def beforeAll(): Unit = analyzerBeforeAll()
  override protected def afterAll(): Unit = analyzerAfterAll()

  test("diet completion pre analyzer length limits don't cutoff terms") {
    // a fictional dish mentioned in Aristophanes' comedy Assemblywomen
    val value = "Lopadotemachoselachogaleokranioleipsanodrimhypotrimmatosilphioparaomelitokatakechymenokichlepikossyphophattoperisteralektryonoptekephalliokigklopeleiolagoiosiraiobaphetraganopterygon"
    val expectedInputValue = value.toLowerCase
    val tokens = CompletionAnalyzer.analyze(value)
    tokens.head should equal(expectedInputValue)
  }

  test("diet completion pre analyzer limit input string length") {
    val value = "It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want of a wife. However little known the feelings or views of such a man may be on his first entering a neighbourhood, this truth is so well fixed in the minds of the surrounding families, that he is considered the rightful property of some one or other of their daughters. My dear Mr. Bennet, said his lady to him one day, have you heard that Netherfield Park is let at last? Mr. Bennet replied that he had not. But it is, returned she; for Mrs. Long has just been here, and she told me all about it. Mr. Bennet made no answer. Do you not want to know who has taken it? cried his wife impatiently. You want to tell me, and I have no objection to hearing it."
    // the config value 64 limits to -------------------------------------------^
    //                plus 14 spaces ---- - - - - - - - - - - - - - - - - - - - - - - - - - ---^
    //                                                              through the end of a term ---^
    // the diet value 32 limits to -------------^
    //               plus 5 spaces ----- - - - - ----^
    //               through the end of a term -------^
    val expectedInputValues = Seq(
      "acknowledged",
      "universally acknowledged",
      "truth universally acknowledged",
      "a truth universally acknowledged",
      "is a truth universally acknowledged",
      "it is a truth universally acknowledged")
    // the config value 32 limits to -^ plus spaces, and a little more to preserve terms
    // the diet value 24 -----^ plus spaces, and a little more to preserve terms
    CompletionAnalyzer.analyze(value) should equal(expectedInputValues)
  }
}
