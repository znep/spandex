import javax.servlet.ServletContext

import com.socrata.spandex.common._
import com.socrata.spandex.http._
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf = new SpandexConfig

  override def init(context: ServletContext): Unit = {
    context.mount(new SpandexServlet(conf), "/*")
  }
}
