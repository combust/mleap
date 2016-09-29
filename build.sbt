import ReleaseTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

name := "mleap"

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

lazy val `root` = project.in(file(".")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(publishArtifact := false).
  enablePlugins(ReleasePlugin).
  aggregate(`mleap-core`, `mleap-runtime`, `mleap-spark`, `bundle-ml`)

lazy val `mleap-core` = project.in(file("mleap-core")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapCoreDependencies)

lazy val `mleap-runtime` = project.in(file("mleap-runtime")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapRuntimeDependencies).
  dependsOn(`mleap-core`, `bundle-ml`)

lazy val `mleap-spark` = project.in(file("mleap-spark")).
  settings(Common.settings).
  settings(Common.combustSettings).
  settings(Common.sonatypeSettings).
  settings(libraryDependencies ++= Dependencies.mleapSparkDependencies).
  dependsOn(`bundle-ml`)

lazy val `bundle-ml` = project.in(file("bundle-ml")).
  settings(Common.settings).
  settings(Common.bundleSettings).
  settings(Common.sonatypeSettings).
  settings(PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value)).
  settings(PB.includePaths in Compile := Seq(file("bundle-protobuf"))).
  settings(PB.protoSources in Compile := Seq(file("bundle-protobuf"))).
  settings(libraryDependencies ++= Dependencies.bundleMlDependencies)