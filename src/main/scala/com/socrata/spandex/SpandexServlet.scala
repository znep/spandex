package com.socrata.spandex

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
