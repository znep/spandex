import javax.servlet.ServletContext

import com.socrata.spandex._
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf: SpandexConfig = new SpandexConfig

  override def init(context: ServletContext) {
    context.mount(new SpandexServlet(conf), "/*")
  }
}
