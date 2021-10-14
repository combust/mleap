logLevel := Level.Warn

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.18")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.0-M8")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.6")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")


libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.1"

addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.11.0")
