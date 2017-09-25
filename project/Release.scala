package ml.combust.mleap

import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, _}
import sbtrelease.ReleaseStateTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

object Release {
  lazy val settings = Seq(releaseVersionBump := sbtrelease.Version.Bump.Minor,
    releaseCrossBuild := true,

    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      setReleaseVersion,
      runClean,
      runTest,
      commitReleaseVersion,
      tagRelease,
      runClean,
      publishArtifacts,
      releaseStepCommand(SonatypeCommand.sonatypeRelease),
      releaseStepTask(publish in autoImport.Docker in MleapProject.serving),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ))
}