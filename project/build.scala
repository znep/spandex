import sbt._
import Keys._
import org.scalastyle.sbt.ScalastylePlugin.scalastyle
import org.scalatra.sbt._
import org.scalatra.sbt.PluginKeys._
import com.earldouglas.xsbtwebplugin.PluginKeys._
import com.earldouglas.xsbtwebplugin.WebPlugin._
import com.mojolly.scalate.ScalatePlugin._
import ScalateKeys._
import sbtassembly.Plugin.AssemblyKeys

object BuildParameters {
  val Organization = "com.socrata"
  val Name = "spandex"
  val Version = "0.1.0-SNAPSHOT"
  val ScalaVersion = "2.10.3"
  val ScalatraVersion = "2.2.2"
  val Conf = config("container")
  val ListenPort = 8042
}

object SpandexBuild extends Build {
  import BuildParameters._
  import Dependencies._

  lazy val styletask = taskKey[Unit]("a task that wraps 'scalastyle' with no input parameters")

  lazy val project = Project (
    "spandex",
    file("."),
    settings = ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      organization := Organization,
      name := Name,
      version := Version,
      scalaVersion := ScalaVersion,
      port in Conf := ListenPort,
      resolvers += Classpaths.typesafeReleases,
      libraryDependencies ++= socrataDeps ++ scalatraDeps ++ jettyDeps ++ testDeps,
      scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature"),
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext", importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
      },
      styletask := { val _ = (scalastyle in Compile).toTask("").value },
      (Keys.`package` in Compile) <<= (Keys.`package` in Compile) dependsOn styletask
    )
  )
}

object Dependencies {
  import BuildParameters._

  lazy val socrataDeps = Nil
  lazy val scalatraDeps = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test"
  )
  lazy val jettyDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % "9.1.5.v20140505" % "container",
    "org.eclipse.jetty" % "jetty-plus" % "9.1.5.v20140505" % "container",
    "javax.servlet" % "javax.servlet-api" % "3.1.0"
  )
  lazy val testDeps = Seq(
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
    "org.scalatest" %% "scalatest" % "2.1.0" % "test"
  )
}