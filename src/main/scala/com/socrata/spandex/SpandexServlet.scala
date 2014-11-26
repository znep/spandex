package com.socrata.spandex

import org.scalatra._
import scalate.ScalateSupport

class SpandexServlet extends SpandexStack {
  private val healthJson = "{\"health\":\"alive\"}"

  get("/") {
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health"){
    healthJson
  }
}
