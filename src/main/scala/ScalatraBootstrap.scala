import javax.servlet.ServletContext

import com.socrata.spandex._
import com.typesafe.config.{ConfigFactory, Config}
import org.scalatra._
import wabisabi.{Client => ElasticsearchClient}

class ScalatraBootstrap extends LifeCycle {
  val conf: SpandexConfig = new SpandexConfig(ConfigFactory.load())

  override def init(context: ServletContext) {
    context.mount(new SpandexServlet(conf), "/*")
  }
}
