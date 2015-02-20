import com.socrata.spandex._
import javax.servlet.ServletContext
import javax.servlet.http.{HttpServletResponse => HttpStatus}
import org.scalatra._
import scala.concurrent.Await
import wabisabi.{Client => ElasticsearchClient}

class ScalatraBootstrap extends LifeCycle {
  val conf = new SpandexConfig

  override def init(context: ServletContext) {
    val _ = ensureIndex
    context.mount(new SpandexServlet(conf), "/*")
  }

  private def ensureIndex: String = {
    val esc = new ElasticsearchClient(conf.esUrl)
    val indexResponse = Await.result(esc.verifyIndex(conf.index), conf.escTimeoutFast)
    val resultHttpCode = indexResponse.getStatusCode
    if (resultHttpCode != HttpStatus.SC_OK) {
      Await.result(esc.createIndex(conf.index, Some(conf.indexSettings)), conf.escTimeoutFast).getResponseBody
    } else {
      indexResponse.getResponseBody
    }
  }
}
