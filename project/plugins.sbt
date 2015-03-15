resolvers ++= Seq(
  "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.mojolly.scalate" % "xsbt-scalate-generator" % "0.5.0")
// scalatra-sbt depends on an old xbst-web-plugin that isn't compatible with newer jetty versions, so force newer xsbt-web-plugin.
addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.9.0")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.3.5")
addSbtPlugin("com.socrata" %% "socrata-sbt-plugins" % "1.4.3")
