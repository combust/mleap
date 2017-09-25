package ml.combust.mleap

import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport
import sbt.Keys._
import sbtrelease.ReleasePlugin.autoImport.{ReleaseStep, _}
import ReleaseTransformations._
import xerial.sbt.Sonatype.SonatypeCommand

object Release {
  lazy val settings = Seq(releaseVersionBump := sbtrelease.Version.Bump.Minor,
    releaseCrossBuild := false, // rely on sbt-doge for cross versions

    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      releaseStepCommand("+ test"),
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepCommand("+ publishSigned"),
      releaseStepCommand(SonatypeCommand.sonatypeRelease),
      releaseStepTask(publish in autoImport.Docker in MleapProject.serving),
      setNextVersion,
      commitNextVersion,
      pushChanges
    ))
}