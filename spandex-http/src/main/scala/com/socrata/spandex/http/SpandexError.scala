package com.socrata.spandex.http

import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

case class SpandexError(message: String,
                        entity: Option[String] = None,
                        source: String = "spandex-http")

object SpandexError {
  implicit val jCode = AutomaticJsonCodecBuilder[SpandexError]
}
