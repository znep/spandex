import javax.servlet.ServletContext

import com.socrata.spandex._
import com.typesafe.config.ConfigFactory
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf: SpandexConfig = new SpandexConfig(ConfigFactory.load())

  override def init(context: ServletContext) {
    context.mount(new SpandexServlet(conf), "/*")
  }
}
