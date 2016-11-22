name := "mleap"

lazy val `root` = project.in(file(".")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(publishArtifact := false).
  enablePlugins(ReleasePlugin).
  aggregate(`mleap-base`, `mleap-core`, `mleap-runtime`,
    `mleap-spark`, `mleap-avro`, `bundle-ml`)

lazy val `mleap-base` = project.in(file("mleap-base")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  enablePlugins(BuildInfoPlugin, GitVersioning).
  settings(buildInfoKeys := Seq[BuildInfoKey](name, version, git.gitHeadCommit),
    buildInfoPackage := "ml.combust",
    buildInfoObject := "BuildValues",
    buildInfoOptions += BuildInfoOption.ToJson)

lazy val `mleap-core` = project.in(file("mleap-core")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapCoreDependencies).
  dependsOn(`mleap-base`)

lazy val `mleap-runtime` = project.in(file("mleap-runtime")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapRuntimeDependencies(scalaVersion.value)).
  dependsOn(`mleap-core`, `bundle-ml`)

lazy val `mleap-spark` = project.in(file("mleap-spark")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapSparkDependencies).
  dependsOn(`mleap-runtime`)

lazy val `mleap-avro` = project.in(file("mleap-avro")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapAvroDependencies).
  dependsOn(`mleap-runtime`)

lazy val `bundle-ml` = project.in(file("bundle-ml")).
  settings(Common.settings).
  settings(Common.bundleSettings).
  settings(Common.sonatypeSettings).
  settings(PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value)).
  settings(PB.includePaths in Compile := Seq(file("bundle-protobuf"))).
  settings(PB.protoSources in Compile := Seq(file("bundle-protobuf"))).
  settings(libraryDependencies ++= Dependencies.bundleMlDependencies).
  dependsOn(`mleap-base`)

import ReleaseTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseCrossBuild := true

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  releaseStepCommand(SonatypeCommand.sonatypeRelease),
  setNextVersion,
  commitNextVersion,
  pushChanges
)