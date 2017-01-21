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

  lazy val defaultSettings = buildSettings ++ sonatypeSettings

  lazy val buildSettings: Seq[Def.Setting[_]] = Seq(
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    fork := true,
    javaOptions in test += sys.env.getOrElse("JVM_OPTS", ""),
    resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    resolvers ++= {
      // Only add Sonatype Snapshots if this version itself is a snapshot version
      if(isSnapshot.value) {
        Seq("Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
      } else {
        Seq()
      }
    }
  )

  lazy val mleapSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.mleap")
  lazy val bundleSettings: Seq[Def.Setting[_]] = Seq(organization := "ml.combust.bundle")

  lazy val sonatypeSettings: Seq[Def.Setting[_]] = Seq(
    sonatypeProfileName := "ml.combust",
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    publishMavenStyle in publishSigned := true,
    publishTo in publishSigned := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
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
      Developer("priannaahsan",
        "Prianna Ahsan",
        "prianna.ahsan@gmail.com",
        url("http://prianna.me")),
      Developer("seme0021",
        "Mikhail Semeniuk",
        "mikhail@combust.ml",
        url("https://www.linkedin.com/in/semeniuk")))
  )
}