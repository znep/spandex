package com.socrata.spandex.http

import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.util.JsonUtil
import com.typesafe.scalalogging.slf4j.Logging
import org.fusesource.scalate.TemplateEngine
import org.fusesource.scalate.layout.DefaultLayoutStrategy
import org.scalatra._
import org.scalatra.scalate.ScalateSupport

import scala.collection.mutable.{Map => MutableMap}

trait SpandexStack extends ScalatraServlet with ScalateSupport with Logging {

  /* wire up the precompiled templates */
  override protected def defaultTemplatePath: List[String] = List("/WEB-INF/templates/views")
  override protected def createTemplateEngine(config: ConfigT) = {
    val engine = super.createTemplateEngine(config)
    engine.layoutStrategy = new DefaultLayoutStrategy(engine,
      TemplateEngine.templateTypes.map("/WEB-INF/templates/layouts/default." + _): _*)
    engine.packagePrefix = "templates"
    engine
  }
  /* end wiring up the precompiled templates */

  override protected def templateAttributes(implicit request: HttpServletRequest): MutableMap[String, Any] = {
    super.templateAttributes ++ MutableMap.empty // Add extra attributes here, they need bindings in the build file
  }

  notFound {
    // remove content type in case it was set through an action
    contentType = ""
    // Try to render a ScalateTemplate if no route matched
    findTemplate(requestPath) map { path =>
      contentType = "text/html"
      layoutTemplate(path)
    } orElse serveStaticResource() getOrElse resourceNotFound()
  }

  error {
    case e: Exception =>
      logger.error("Exception was thrown", e)
      InternalServerError(JsonUtil.renderJson(SpandexError(e)))
  }
}
