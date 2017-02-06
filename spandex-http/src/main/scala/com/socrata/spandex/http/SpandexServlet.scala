package com.socrata.spandex.http

import javax.servlet.http.{HttpServletResponse => HttpStatus}

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.spandex.common._
import com.socrata.spandex.common.client._
import com.socrata.spandex.http.SpandexResult.Fields._
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.common.unit.Fuzziness

import scala.util.Try

class SpandexServlet(conf: SpandexConfig,
                     client: => SpandexElasticSearchClient) extends SpandexStack {
  logger.info("Configuration:\n" + conf.debugString)

  def index: String = conf.es.index

  val version = BuildInfo.toJson

  def columnMap(datasetId: String, copyNum: Long, userColumnId: String): ColumnMap =
    client.columnMap(datasetId, copyNum, userColumnId).getOrElse(halt(
      HttpStatus.SC_NOT_FOUND, JsonUtil.renderJson(SpandexError("Column not found", Some(userColumnId)))))
  def urlDecode(s: String): String = java.net.URLDecoder.decode(s, EncodingUtf8)

  healthCheck("alive") {true}
  healthCheck("version") {Try {version}}
  healthCheck("esClusterHealth") {Try {esClusterHealth}}

  get("/version") {
    logger.info(">>> /version")
    contentType = ContentTypeJson
    logger.info(s"<<< $version")
    version
  }

  get("/") {
    // TODO: getting started and/or quick reference
    <html>
      <body>
        <h1>Hello, spandex</h1>
      </body>
    </html>
  }

  def esClusterHealth: String = {
    val clusterAdminClient = client.client.admin().cluster()
    val req = new ClusterHealthRequest(index)
    clusterAdminClient.health(req).actionGet().toString
  }

  // looks like: /suggest/alpha.1234/latest/abcd-1234/<sometext> where abcd-1234 is the column 4x4
  get(s"/$routeSuggest/:$paramDatasetId/:$paramStageInfo/:$paramUserColumnId/:$paramText") {
    timer("suggestText") {
      suggest { (col, text, fuzz, size) =>
        SpandexResult(client.suggest(col, size, text, fuzz, conf.suggestFuzzLength, conf.suggestFuzzPrefix))
      }
    }.call()
  }

  /* How to get all the results out of Lucene.
   * Ignore the provided text and fuzziness parameters and replace as follows.
   * Text "1 character" => blank string is not allowed, but through the fuzziness below
   *                       this 1 character will be factored out.
   * Fuzziness ONE => approximately allows 1 edit distance from the given to result texts.
   * Fuzz Length 0 => start giving fuzzy results from any length of input text.
   * Fuzz Prefix 0 => allow all results no matter how badly matched.
   * TA-DA!
   */
  private[this] val sampleText = "a"
  private[this] val sampleFuzz = Fuzziness.ONE
  private[this] val sampleFuzzLen = 0
  private[this] val sampleFuzzPre = 0
  // looks like: /suggest/alpha.1234/latest/abcd-1234 where abcd-1234 is the column 4x4
  get(s"/$routeSuggest/:$paramDatasetId/:$paramStageInfo/:$paramUserColumnId") {
    timer("suggestSample") {
      params.get(paramText) match {
        case None => suggest { (col, _, _, size) =>
          SpandexResult(client.suggest(col, size, sampleText, sampleFuzz, sampleFuzzLen, sampleFuzzPre))
        }
        case Some(s) => suggest { (col, text, fuzz, size) =>
          SpandexResult(client.suggest(col, size, text, fuzz, conf.suggestFuzzLength, conf.suggestFuzzPrefix))
        }
      }
    }.call()
  }

  /* Deletes all artifacts associated with a particular dataset from Spandex.
   *
   * @param paramDatasetId the dataset system ID for the dataset to delete
   * @return `Map[String, Int]` if successful, indicating how many of each type was deleted from the index
   *
   * @note: When deleting datasets from Spandex, be sure to delete the corresponding entry from data
   * coordinator. If you don't, if the dataset in question gets any updates, Spandex will fail to
   * index it and we'll get a broken dataset alert.
   */
  delete(s"/$routeSuggest/:$paramDatasetId") {
    timer("deleteDataset") {
      contentType = ContentTypeJson
      val datasetId = params.getOrElse(paramDatasetId, halt(HttpStatus.SC_BAD_REQUEST))
      JsonUtil.renderJson(client.deleteDatasetById(datasetId))
    }.call()
  }

  def copyNum(datasetId: String, stageInfoText: String): Long = {
    Stage(stageInfoText) match {
      case Some(Number(n)) => n
      case Some(stage: Stage) =>
        client.datasetCopyLatest(datasetId, Some(stage)).map(_.copyNumber).getOrElse(
          halt(HttpStatus.SC_NOT_FOUND, JsonUtil.renderJson(SpandexError("copy not found", Some(stageInfoText))))
        )
      case _ => halt(HttpStatus.SC_BAD_REQUEST, JsonUtil.renderJson(SpandexError("stage invalid", Some(stageInfoText))))
    }
  }

  // scalastyle:ignore method.length
  def suggest(f: (ColumnMap, String, Fuzziness, Int) => SpandexResult): String = {
    logger.info(s">>> $requestPath params: $params")

    contentType = ContentTypeJson
    val datasetId = params.getOrElse(paramDatasetId, halt(HttpStatus.SC_BAD_REQUEST))
    val stageInfoText = params.getOrElse(paramStageInfo, halt(HttpStatus.SC_BAD_REQUEST))
    val userColumnId = params.getOrElse(paramUserColumnId, halt(HttpStatus.SC_BAD_REQUEST))
    val text = urlDecode(params.getOrElse(paramText, ""))
    logger.info(s"urlDecoded param text -> $text")

    val fuzz = Fuzziness.build(params.getOrElse(paramFuzz, conf.suggestFuzziness))
    val size = params.get(paramSize).headOption.fold(conf.suggestSize)(_.toInt)

    // this results in an max aggregation with a sort
    //
    // {
    //   "query": {
    //     "bool": {
    //       "must": [
    //         {
    //           "term": {
    //             "stage": "<STAGE>"
    //           },
    //           "term": {
    //             "dataset_id": "<DATASET_ID>"
    //           }
    //         }
    //       ]
    //     }
    //   },
    //   "size": 1,
    //   "sort": {
    //     "copy_number": "desc"
    //   },
    //   "aggregations": {
    //     "latest_copy": {
    //       "max": {
    //         "field": "copy_number"
    //       }
    //     }
    //   }
    // }
    val copy = copyNum(datasetId, stageInfoText)
    logger.info(s"found copy $copy")

    // this results in a fetch by composite ID
    val column = columnMap(datasetId, copy, userColumnId)
    logger.info(s"found column $column")

    // this results in a suggest query
    //
    // {
    //   "suggest": {
    //     "completion": {
    //       "context": {
    //         "composite_id": <COMPOSITE_ID>
    //       }
    //       "field": "value",
    //       "text": <TEXT>,
    //       "size": 10
    //     }
    //   }
    // }
    // https://www.elastic.co/guide/en/elasticsearch/reference/1.7/search-suggesters-completion.html
    // http://blog.mikemccandless.com/2010/12/using-finite-state-transducers-in.html
    val result = f(column, text, fuzz, size)
    logger.info(s"<<< $result")
    JsonUtil.renderJson(result)
  }
}
