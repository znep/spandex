import javax.servlet.ServletContext

import com.socrata.spandex._
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    context.mount(new SpandexServlet, "/*")
  }
}
