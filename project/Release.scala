package ml.combust.mleap

import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, _}
import sbtrelease.ReleaseStateTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

object Release {
  lazy val settings = Seq(releaseVersionBump := sbtrelease.Version.Bump.Minor,
    releaseCrossBuild := true,

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
    ))
}