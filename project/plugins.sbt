logLevel := Level.Warn

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.1")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.6.1")

libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.5.42"