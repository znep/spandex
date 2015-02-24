resolvers ++= Seq(Classpaths.sbtPluginReleases, Classpaths.typesafeReleases,
  "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases"
)

addSbtPlugin("com.mojolly.scalate" % "xsbt-scalate-generator" % "0.5.0")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.3.5")
addSbtPlugin("com.socrata" %% "socrata-sbt-plugins" % "1.4.1")
