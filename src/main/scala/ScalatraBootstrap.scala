import javax.servlet.ServletContext

import com.socrata.spandex._
import com.typesafe.config.{ConfigFactory, Config}
import org.scalatra._
import wabisabi.{Client => ElasticsearchClient}

class ScalatraBootstrap extends LifeCycle {
  val conf: Config = ConfigFactory.load()
  val esUrl: String = conf.getString("spandex.elasticsearch.url")

  val elasticsearch = new ElasticsearchClient(esUrl)

  override def init(context: ServletContext) {
    context.mount(new SpandexServlet(elasticsearch), "/*")
  }
}
