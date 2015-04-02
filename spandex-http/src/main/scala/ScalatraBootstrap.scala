import javax.servlet.ServletContext

import com.socrata.spandex.common.SpandexConfig
import com.socrata.spandex.common.client.SpandexElasticSearchClient
import com.socrata.spandex.http.SpandexServlet
import org.scalatra._

class ScalatraBootstrap extends LifeCycle {
  val conf = new SpandexConfig

  val client = new SpandexElasticSearchClient(conf.es)

  override def init(context: ServletContext): Unit = {
    context.mount(new SpandexServlet(conf, client), "/*")
  }

  override def destroy(context: ServletContext): Unit = {
    client.close()
  }
}
