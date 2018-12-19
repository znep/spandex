import com.earldouglas.xwp.XwpPlugin
import com.mojolly.scalate.ScalatePlugin.ScalateKeys._
import com.mojolly.scalate.ScalatePlugin._
import org.scalatra.sbt._
import pl.project13.scala.sbt.JmhPlugin
import sbt.Keys._
import sbt._
import sbtbuildinfo.BuildInfoKeys.buildInfoPackage
import sbtbuildinfo.BuildInfoPlugin
import scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages
import sbtassembly.AssemblyKeys._
import sbtassembly.{AssemblyPlugin, MergeStrategy}


object SpandexBuild extends Build {
  val Name = "com.socrata.spandex"
  val JettyListenPort = 8042 // required for container embedded jetty

  val dependenciesSnippet = SettingKey[xml.NodeSeq]("dependencies-snippet")

  lazy val commonSettings = Seq(
    organization := "com.socrata",
    scalaVersion := "2.10.5",
    fork in Test := true,
    testOptions in Test += Tests.Argument("-oF"),
    resolvers ++= Deps.resolverList,
    dependenciesSnippet := <xml:group/>,
    ivyXML <<= dependenciesSnippet { snippet =>
      <dependencies>
      {snippet.toList}
      <exclude org="commons-logging" module="commons-logging-api"/>
      <exclude org="commons-logging" module="commons-logging"/>
      </dependencies>
    },
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case "META-INF/io.netty.versions.properties" => MergeStrategy.last
      case "application.conf" => MergeStrategy.last
      case other => MergeStrategy.defaultMergeStrategy(other)
    },
    // Make sure the "configs" dir is on the runtime classpaths so application.conf can be found.
    fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") },
    fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d.getParentFile / "configs") }
  )

  lazy val build = Project(
    "spandex",
    file("."),
    settings = commonSettings
  ).aggregate(spandexCommon, spandexHttp, spandexSecondary, spandexDataLoader, spandexIntegrationTests)
    .dependsOn(spandexCommon, spandexHttp, spandexSecondary, spandexDataLoader, spandexIntegrationTests)

  lazy val spandexCommon = Project(
    "spandex-common",
    file("./spandex-common/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondaryFiltered
    )
  ).disablePlugins(JmhPlugin)

  lazy val spandexHttp = Project(
    "spandex-http",
    file("./spandex-http/"),
    settings = commonSettings ++ ScalatraPlugin.scalatraWithJRebel ++ scalateSettings ++ Seq(
      buildInfoPackage := "com.socrata.spandex.http",
      libraryDependencies ++= Deps.socrata ++ Deps.http ++ Deps.test ++ Deps.common,
      coverageExcludedPackages := "<empty>;templates.*;" + coverageExcludedPackages.value,
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
      }
    ) ++ XwpPlugin.jetty(port = JettyListenPort)
  ).dependsOn(spandexCommon % "compile;test->test")
    .disablePlugins(JmhPlugin)
    .enablePlugins(BuildInfoPlugin)

  lazy val spandexSecondary = Project(
    "spandex-secondary",
    file("./spandex-secondary/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondary,
      fullClasspath in Test <+= baseDirectory map { d => Attributed.blank(d / "config") },
      fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "config") }
    )
  ).dependsOn(spandexCommon % "compile;test->test")
    .disablePlugins(JmhPlugin)

  lazy val spandexDataLoader = Project(
    "spandex-data-loader",
    file("./spandex-data-loader/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.common ++ Deps.dataLoader,
      buildInfoPackage := "com.socrata.spandex.data",
      mainClass in assembly := Some("com.socrata.spandex.data.Loader")
    )
  ).dependsOn(spandexCommon % "compile")
    .enablePlugins(BuildInfoPlugin)

  lazy val spandexIntegrationTests = Project(
    "spandex-integration-tests",
    file("./spandex-integration-tests/"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Deps.socrata ++ Deps.test ++ Deps.common ++ Deps.secondary,
      fullClasspath in Runtime <+= baseDirectory map { d => Attributed.blank(d / "config") },
      parallelExecution in ThisBuild := false
    )
  ).dependsOn(
    spandexCommon % "compile;test->test",
    spandexHttp % "compile;test->test",
    spandexSecondary % "compile;test->test",
    spandexDataLoader % "compile;test->test"
  ).disablePlugins(JmhPlugin)

  lazy val gitSha = Process(Seq("git", "describe", "--always", "--dirty", "--long", "--abbrev=10")).!!.stripLineEnd
}

object Deps {
  val ScalatraVersion = "2.4.0.RC1"
  val JettyVersion = "9.2.10.v20150310"

  lazy val resolverList = Seq(
    "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases", // scalaz-stream used in scalatra 2.4.x
    "socrata artifactory" at "https://repo.socrata.com/artifactory/libs-release",
    "Elasticsearch releases" at "https://artifacts.elastic.co/maven"
  )

  lazy val socrata = Seq(
    "com.rojoma" %% "rojoma-json-v3" % "3.3.0",
    "com.rojoma" %% "simple-arm" % "1.2.0",
    "com.rojoma" %% "simple-arm-v2" % "2.1.0",
    "com.socrata" %% "soql-types" % "2.11.4"
      excludeAll(ExclusionRule(organization = "com.rojoma"),
                 ExclusionRule(organization = "commons-io"))
  )

  lazy val http = Seq(
    "org.scalatra" %% "scalatra" % ScalatraVersion,
    "org.scalatra" %% "scalatra-scalate" % ScalatraVersion,
    "org.scalatra" %% "scalatra-metrics" % ScalatraVersion,
    "ch.qos.logback" % "logback-classic" % "1.1.3" % "runtime",
    "org.eclipse.jetty" % "jetty-webapp" % JettyVersion % "container;compile",
    "org.eclipse.jetty" % "jetty-plus" % JettyVersion % "container"
  )

  lazy val test = Seq(
    "org.scalatra" %% "scalatra-specs2" % ScalatraVersion % "test",
    "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
    "org.apache.logging.log4j" % "log4j-api" % "2.7",
    "org.apache.logging.log4j" % "log4j-core" % "2.7"
  )

  lazy val common = Seq(
    "javax.servlet" % "javax.servlet-api" % "3.1.0",
    "com.typesafe" % "config" % "1.2.1",
    "com.typesafe" %% "scalalogging-slf4j" % "1.1.0",
    "commons-codec" % "commons-codec" % "1.10",
    "commons-io" % "commons-io" % "2.4",
    "org.elasticsearch" % "elasticsearch" % "5.4.1",
    "org.elasticsearch.client" % "x-pack-transport" % "5.4.1"
  )

  lazy val secondary = Seq(
    "com.socrata" %% "secondarylib" % "3.4.36"
  )

  lazy val dataLoader = Seq(
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.8.8",
    "com.github.scopt" %% "scopt" % "3.7.0"
  )

  lazy val secondaryFiltered = secondary.map(
    _.exclude("org.slf4j", "slf4j-log4j12").excludeAll(ExclusionRule(organization = "com.rojoma"))
  )
}
