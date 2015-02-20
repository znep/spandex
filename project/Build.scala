import sbt._
import sbt.Keys._
import org.scalastyle.sbt.ScalastylePlugin.scalastyle
import org.scalatra.sbt._
import com.earldouglas.xsbtwebplugin.PluginKeys.port
import com.mojolly.scalate.ScalatePlugin._
import com.mojolly.scalate.ScalatePlugin.ScalateKeys._
import sbtassembly.AssemblyKeys._
import sbtassembly.MergeStrategy

object BuildParameters {
  val Organization = "com.socrata"
  val Name = "spandex"
  val Version = "0.1.0-SNAPSHOT"
  val ScalaVersion = "2.10.4"
  val ScalatraVersion = "2.3.0"
  val JettyVersion = "9.2.1.v20140609" // pinned to this version in Scalatra
  val Conf = config("container")
  val ListenPort = 8042 // required for container embedded jetty
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
      resolvers ++= resolverList,
      libraryDependencies ++= socrataDeps ++ scalatraDeps ++ jettyDeps ++ testDeps ++ miscDeps,
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
      test in assembly := {},
      assemblyMergeStrategy in assembly := {
          case "about.html" => MergeStrategy.first
          case x =>
            val old = (assemblyMergeStrategy in assembly).value
            old(x)
      },
      resourceGenerators in Compile <+= (resourceManaged, baseDirectory) map {
        (managed, base) =>
          val webappBase = base / "src" / "main" / "webapp"
          for {
            (from, to) <- webappBase ** "*" pair rebase(webappBase, managed / "main" / "webapp")
          } yield {
            Sync.copy(from,to)
            to
          }
      },
      styletask := { val _ = (scalastyle in Compile).toTask("").value },
      assembly <<= assembly dependsOn styletask,
      (Keys.`package` in Compile) <<= (Keys.`package` in Compile) dependsOn styletask
    )
  )
}

object Dependencies {
  import BuildParameters._

  lazy val resolverList = Seq(
    "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
    "socrata releases" at "http://repository-socrata-oss.forge.cloudbees.com/release",
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases",
    Classpaths.sbtPluginReleases,
    Classpaths.typesafeReleases
  )

  lazy val socrataDeps = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.2.2",
    "com.rojoma" %% "simple-arm" % "1.2.0"
  )
  lazy val scalatraDeps = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion
  )
  lazy val jettyDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % JettyVersion % "container;compile",
    "org.eclipse.jetty" % "jetty-plus" % JettyVersion % "container",
    "javax.servlet" % "javax.servlet-api" % "3.1.0"
  )
  lazy val testDeps = Seq(
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
    "org.scalatest" %% "scalatest" % "2.1.0" % "test"
  )
  lazy val miscDeps = Seq(
    "com.typesafe" % "config" % "1.2.1",
    "wabisabi" %% "wabisabi" % "2.0.15"
  )
}
