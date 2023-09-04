logLevel := Level.Warn

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.21")
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0")
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.0.1")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.13"

addSbtPlugin("com.frugalmechanic" % "fm-sbt-s3-resolver" % "0.21.0")

addDependencyTreePlugin
