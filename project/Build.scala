import com.earldouglas.xsbtwebplugin.PluginKeys.port
import com.mojolly.scalate.ScalatePlugin.ScalateKeys._
import com.mojolly.scalate.ScalatePlugin._
import org.scalatra.sbt._
import sbt.Keys._
import sbt._
import sbtbuildinfo.{BuildInfoOption, BuildInfoKey, BuildInfoPlugin}
import sbtbuildinfo.BuildInfoKeys._
import scoverage.ScoverageSbtPlugin.ScoverageKeys.{coverageExcludedPackages, coverageMinimum, coverageFailOnMinimum}

object SpandexBuild extends Build {
  val Name = "com.socrata.spandex"
  val ScalaVersion = "2.10.4"
  val JettyConf = config("container")
  val JettyListenPort = 8042 // required for container embedded jetty

  lazy val commonSettings = Seq(
    scalaVersion := ScalaVersion,
    resolvers ++= Deps.resolverList
  )

  lazy val build = Project(
    "spandex",
    file("."),
    settings = commonSettings
  ).aggregate(spandexCommon, spandexHttp, spandexSecondary).
    dependsOn(spandexCommon, spandexHttp, spandexSecondary)

  lazy val spandexCommon = Project (
    "spandex-common",
    file("./spandex-common/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondary,
      fullClasspath in Runtime += Attributed.blank(baseDirectory.value / ".." / "esconfigs")
    )
  )

  lazy val spandexHttp = Project (
    "spandex-http",
    file("./spandex-http/"),
    settings = commonSettings ++ ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      buildInfoKeys := Seq[BuildInfoKey](
        name,
        version,
        scalaVersion,
        sbtVersion,
        BuildInfoKey.action("buildTime") { System.currentTimeMillis },
        BuildInfoKey.action("revision") { gitSha }),
      buildInfoPackage := "com.socrata.spandex.http",
      buildInfoOptions += BuildInfoOption.ToMap,
      libraryDependencies ++= Deps.socrata ++ Deps.http ++ Deps.test ++ Deps.common,
      port in JettyConf := JettyListenPort,
      coverageExcludedPackages := "<empty>;templates.*",
      scalateTemplateConfig in Compile <<= (sourceDirectory in Compile){ base =>
        Seq(
          TemplateConfig(
            base / "webapp" / "WEB-INF" / "templates",
            Seq.empty,  /* default imports should be added here */
            Seq(
              Binding("context", "_root_.org.scalatra.scalate.ScalatraRenderContext",
                importMembers = true, isImplicit = true)
            ),  /* add extra bindings here */
            Some("templates")
          )
        )
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
      fork in Test := true
    )
  ).enablePlugins(BuildInfoPlugin).dependsOn(spandexCommon % "compile;test->test")

  lazy val spandexSecondary = Project (
    "spandex-secondary",
    file("./spandex-secondary/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondary
    )
  ).dependsOn(spandexCommon % "compile;test->test")

  lazy val gitSha = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.stripLineEnd
}

object Deps {
  val ScalatraVersion = "2.3.0"
  val JettyVersion = "9.2.1.v20140609" // pinned to this version in Scalatra

  lazy val resolverList = Seq(
    "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
  )

  lazy val socrata = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.2.2",
    "com.rojoma" %% "simple-arm" % "1.2.0",
    "com.socrata" %% "soql-types" % "0.5.0"
  )
  lazy val http = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
    "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % JettyVersion % "container;compile",
    "org.eclipse.jetty" % "jetty-plus" % JettyVersion % "container"
  )
  lazy val test = Seq(
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test"
  )
  lazy val common = Seq(
    "javax.servlet" % "javax.servlet-api" % "3.1.0",
    "com.typesafe" % "config" % "1.2.1",
    "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
    "commons-io" % "commons-io" % "1.4",
    "org.elasticsearch" % "elasticsearch" % "1.4.4"
  )
  lazy val secondary = Seq(
    "com.socrata" %% "secondarylib" % "0.6.2" exclude("org.slf4j", "slf4j-log4j12")
  )
}
