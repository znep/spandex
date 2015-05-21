package com.socrata.spandex.common.client

import scala.util.{Success, Try}

sealed trait Stage {
  def name: String = this.toString
}

case object Unpublished extends Stage
case object Published extends Stage
case object Snapshotted extends Stage
case object Discarded extends Stage
case object Latest extends Stage
case class Number(n: Long) extends Stage

object Stage {
  def apply(stage: String): Option[Stage] = {
    stage.toLowerCase match {
      case ""             => Some(Latest)
      case "unpublished"  => Some(Unpublished)
      case "published"    => Some(Published)
      case "snapshotted"  => Some(Snapshotted)
      case "discarded"    => Some(Discarded)
      case "latest"       => Some(Latest)
      case s: String      =>
        Try {s.toLong} match {
          case Success(n) => Some(Number(n))
          case _          => None
        }
    }
  }
}
