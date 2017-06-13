package com.socrata.spandex.common

import scala.io.Source

trait MarvelNames {
  val source = Source.fromFile("src/main/resources/names.txt", "utf-8")
  val names: Array[String] = source.getLines().toArray
  val namesKeyword: Array[String] = names.map(_.replaceAll("""\W+""", "_"))

  private[this] var i: Int = 0
  private[this] var j: Int = 0

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
