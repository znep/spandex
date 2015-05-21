package com.socrata.spandex.common

import scala.io.Source

trait MarvelNames {
  val names: Array[String] = Source.fromFile("../esconfigs/names.txt", "utf-8").getLines().toArray
  val namesKeyword: Array[String] = names.map(_.replaceAll("""\W+""", "_"))

  private var i: Int = 0
  private var j: Int = 0

  def anotherPhrase: String = {
    val name = names(i)
    i = (i + 1) % names.length
    name
  }

  def anotherKeyword: String = {
    val name = namesKeyword(i)
    j = (j + 1) % namesKeyword.length
    name
  }
}
