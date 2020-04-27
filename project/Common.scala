package ml.combust.mleap

import sbt._
import Keys._
import com.typesafe.sbt.SbtPgp.autoImportImpl._
import com.typesafe.sbt.pgp.PgpKeys._
import sbtrelease.ReleasePlugin.autoImport._
import xerial.sbt.Sonatype.autoImport._

object Common {
  lazy val defaultMleapSettings = defaultSettings ++ mleapSettings ++ sonatypeSettings
  lazy val defaultBundleSettings = defaultSettings ++ bundleSettings ++ sonatypeSettings
  lazy val defaultMleapXgboostSparkSettings = defaultMleapSettings ++ sonatypeSettings
  lazy val defaultMleapServingSettings = defaultMleapSettings ++ noPublishSettings

  lazy val defaultSettings = buildSettings ++ sonatypeSettings

  lazy val buildSettings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := "2.12.10",
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    fork in Test := true,
    javaOptions in test += sys.env.getOrElse("JVM_OPTS", ""),
    resolvers += Resolver.mavenLocal,
    resolvers ++= {
      // Only add Sonatype Snapshots if this version itself is a snapshot version
      if(isSnapshot.value) {
        Seq(
          "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
          "ASF spark" at "https://repository.apache.org/content/repositories/orgapachespark-1341"
        )
      } else {
        Seq()
      }
    }
  )

  lazy val mleapSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.mleap")
  lazy val bundleSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.bundle")

  lazy val noPublishSettings: Seq[Def.Setting[_]] = Seq(
    publishTo in publishSigned := None,
    publishTo := None
  )

  lazy val sonatypeSettings: Seq[Def.Setting[_]] = Seq(
    sonatypeProfileName := "ml.combust",
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishMavenStyle := true,
    publishTo := Some({
      if (isSnapshot.value) {
        Opts.resolver.sonatypeSnapshots
      } else {
        Opts.resolver.sonatypeStaging
      }
    }),
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    licenses := Seq("Apache 2.0 License" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("https://github.com/combust/mleap")),
    scmInfo := Some(ScmInfo(url("https://github.com/combust/mleap.git"),
      "scm:git:git@github.com:combust/mleap.git")),
    developers := List(Developer("hollinwilkins",
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
        url("https://www.linkedin.com/in/anca-sarb")))
  )
}
