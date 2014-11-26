resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases"

addSbtPlugin("com.mojolly.scalate" % "xsbt-scalate-generator" % "0.5.0")

addSbtPlugin("org.scalatra.sbt" % "scalatra-sbt" % "0.3.5")

addSbtPlugin("com.socrata" % "socrata-cloudbees-sbt" % "1.3.1")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")