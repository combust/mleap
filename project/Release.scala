package ml.combust.mleap

import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, _}
import ReleaseTransformations._

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
      releaseStepCommand("sonatypeRelease"),
      releaseStepTask(publish in autoImport.Docker in MleapProject.serving),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ))
}