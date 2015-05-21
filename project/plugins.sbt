resolvers ++= Seq(
  "socrata releases" at "https://repository-socrata-oss.forge.cloudbees.com/release"
)

addSbtPlugin("com.mojolly.scalate" % "xsbt-scalate-generator" % "0.5.0")
addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "1.1.0")
addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.4.0")
addSbtPlugin("com.socrata" %% "socrata-sbt-plugins" % "1.4.4")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.4.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.1.15")
