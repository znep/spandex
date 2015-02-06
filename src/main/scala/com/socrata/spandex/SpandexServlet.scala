package com.socrata.spandex

import javax.servlet.http.{HttpServletResponse => HttpStatus}

class SpandexServlet extends SpandexStack {

  get("//?") {
    // TODO: spandex getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health/?"){
    // TODO: report health
    "{\"health\":\"alive\"}"
  }

  get ("/add/:4x4/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add mappings
    """{
      | "acknowledged":true,
      | "4x4":"%s"
      |}
    """.stripMargin.format(fourbyfour)
  }

  get ("/add/:4x4/:col/?"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add mappings
    """{
      | "acknowledged":true,
      | "4x4":"%s",
      | "col":"%s"
      |}
    """.stripMargin.format(fourbyfour, column)
  }

  get ("/syn/:4x4"){
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch delete mappings and documents
    """{
      | "acknowledged":true,
      | "4x4":"%s"
      |}
    """.stripMargin.format(fourbyfour)
  }

  get ("/suggest/:4x4/:col/:txt") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    val column = params.getOrElse("col", halt(HttpStatus.SC_BAD_REQUEST))
    val querytext = params.getOrElse("txt", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch query
    val expectedanswer = """{"phrase":"NARCOTICS", "weight":1.0}"""
    """{
      | "4x4":"%s",
      | "col":"%s",
      | "text":"%s",
      | "suggestions": [%s]
      |}
    """.stripMargin.format(fourbyfour, column, querytext, expectedanswer)
  }

  post("/ver/:4x4") {
    val fourbyfour = params.getOrElse("4x4", halt(HttpStatus.SC_BAD_REQUEST))
    // TODO: elasticsearch add documents to index
    val updates = request.body
    """{
      | "acknowledged":true,
      | "4x4":"%s",
      | "updates:" [%s]
      |}
    """.stripMargin.format(fourbyfour, updates)
  }
}
