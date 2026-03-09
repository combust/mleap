package ml.combust.mleap

import sbt._
import Keys._

object Common {
  lazy val defaultMleapSettings = defaultSettings ++ mleapSettings
  lazy val defaultBundleSettings = defaultSettings ++ bundleSettings
  lazy val defaultMleapXgboostSparkSettings = defaultMleapSettings
  lazy val defaultMleapServingSettings = defaultMleapSettings ++ noPublishSettings

  lazy val defaultSettings = buildSettings ++ publishSettings

  lazy val buildSettings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := "2.13.16",
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-release", "17"),
    Test / fork := true,
    javaOptions ++= Seq(
      "--add-opens", "java.base/java.nio=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens", "java.base/java.util=ALL-UNNAMED"
    ),
    Test / javaOptions ++= Seq(
      "--add-opens", "java.base/java.nio=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang=ALL-UNNAMED",
      "--add-opens", "java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens", "java.base/java.util=ALL-UNNAMED"
    ),
    ThisBuild / libraryDependencySchemes +=
      "org.scala-lang.modules" %% "scala-collection-compat" % VersionScheme.Always,
    resolvers += Resolver.mavenLocal,
    resolvers ++= {
      if (isSnapshot.value) {
        Seq(
          "Sonatype Central Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/",
          "ASF Snapshots" at "https://repository.apache.org/content/groups/snapshots"
        )
      } else {
        Seq()
      }
    }
  )

  lazy val mleapSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.mleap")
  lazy val bundleSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.bundle")

  lazy val noPublishSettings: Seq[Def.Setting[_]] = Seq(
    publish / skip := true
  )

  // Metadata required for Maven Central — sbt-ci-release handles
  // version, publishTo, publishMavenStyle, and credentials automatically.
  lazy val publishSettings: Seq[Def.Setting[_]] = Seq(
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0 License" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("https://github.com/combust/mleap")),
    scmInfo := Some(ScmInfo(
      url("https://github.com/combust/mleap.git"),
      "scm:git:git@github.com:combust/mleap.git"
    )),
    developers := List(
      Developer("hollinwilkins",
        "Hollin Wilkins",
        "hollinrwilkins@gmail.com",
        url("http://hollinwilkins.com")),
      Developer("seme0021",
        "Mikhail Semeniuk",
        "mikhail@combust.ml",
        url("https://www.linkedin.com/in/semeniuk")),
      Developer("ancasarb",
        "Anca Sarb",
        "sarb.anca@gmail.com",
        url("https://www.linkedin.com/in/anca-sarb"))
    )
  )
}