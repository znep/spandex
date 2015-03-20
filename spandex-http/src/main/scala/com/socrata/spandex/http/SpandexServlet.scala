package com.socrata.spandex.http

import com.socrata.spandex.common._

class SpandexServlet(conf: SpandexConfig) extends SpandexStack {
  get("//?") {
    // TODO: com.socrata.spandex.secondary getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  get ("/health/?"){
    // TODO
  }

  get ("/suggest/:4x4/:col/:txt/?") {
    val fourbyfour: String = params.getOrElse("4x4", "")
    val column: String = params.getOrElse("col", "")
    val text: String = params.getOrElse("txt", "")
    // TODO : Map 4x4 and user-facing column name to internal IDs that we store in ES
    //        (This may involving the whole flow being routed through Soda Fountain)
    //
    // TODO : Formulate ES query
    /*
    {
      "field_value": {
          "text":"gor",
          "completion": {
            "field": "value",
            "context" : { "composite_id" : "primus.1234|1|abcd-2345" },
            "fuzzy" : { "fuzziness" : 2 }
          }
      }
    }
    */
  }
}
