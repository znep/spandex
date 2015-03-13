package com.socrata.spandex.http

import com.socrata.spandex.common._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

object Spandex extends App {
  lazy val conf = new SpandexConfig
  var ready: Boolean = false

  val esRouter = new ElasticsearchServer(conf.esPort)
  esRouter.start()

  SpandexBootstrap.ensureIndex(conf, conf.esPort)

  val port = conf.spandexPort
  val pathRoot = "/"

  val context = new WebAppContext
  context.setContextPath(pathRoot)
  context.setResourceBase("src/main/webapp")
  context.addEventListener(new ScalatraListener)
  context.addServlet(classOf[DefaultServlet], pathRoot)

  val server = new Server(port)
  server.setHandler(context)
  server.start()
  esRouter.waitForReady()
  ready = true
  server.join()

  esRouter.stop()
}
