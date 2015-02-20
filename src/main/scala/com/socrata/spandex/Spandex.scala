package com.socrata.spandex

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

object Spandex extends App {
  val conf = new SpandexConfig

  override def main(args: Array[String]): Unit = {
    val port = conf.port

    val context = new WebAppContext
    context.setContextPath("/")
    context.setResourceBase("src/main/webapp")
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")

    val server = new Server(port)
    server.setHandler(context)
    server.start()
    server.join()
  }
}
