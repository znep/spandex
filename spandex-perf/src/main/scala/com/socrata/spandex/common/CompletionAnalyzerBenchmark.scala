package com.socrata.spandex.common

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

import scala.io.Source

// scalastyle:off magic.number
@State(Scope.Thread)
class CompletionAnalyzerBenchmark {
  val names: Array[String] = Source.fromFile("../esconfigs/names.txt", "utf-8").getLines().toArray
  private var i: Int = 0
  def anotherName: String = {
    val name = names(i)
    i = (i + 1) % names.length
    name
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  def analyzeCompletionTokens(): Unit = {
    @annotation.tailrec
    def go(n: Int): Unit = {
      if (n > 0) {
        CompletionAnalyzer.analyze(anotherName)
        go(n - 1)
      }
    }
    go(10000)
  }
}
