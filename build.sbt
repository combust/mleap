name := "mleap"

updateOptions := updateOptions.value.withCachedResolution(true)

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseCrossBuild := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value

lazy val `root` = project.in(file(".")).
  settings(Common.settings).
  settings(Common.combustSettings).
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
//  settings(sourceDirectories in ScalaPbPlugin.protobufConfig := Seq(file("bundle-protobuf"))).
//  settings(ScalaPbPlugin.includePaths in ScalaPbPlugin.protobufConfig := Seq(file("bundle-protobuf"))).
  settings(libraryDependencies ++= Dependencies.bundleMlDependencies)