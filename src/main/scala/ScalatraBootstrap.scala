import javax.servlet.ServletContext

import com.socrata.spandex._
import org.scalatra._
import wabisabi.{Client => ElasticsearchClient}

class ScalatraBootstrap extends LifeCycle {
  // TODO: config value for elasticsearch cluster url
  val elasticsearch = new ElasticsearchClient("http://eel:9200")

  override def init(context: ServletContext) {
    context.mount(new SpandexServlet(elasticsearch), "/*")
  }
}
