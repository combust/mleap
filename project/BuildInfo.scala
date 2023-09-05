package ml.combust.mleap

import com.github.sbt.git.GitPlugin.autoImport._
import sbt.Keys._
import sbtbuildinfo.BuildInfoPlugin.autoImport._

object BuildInfo {
  lazy val settings = Seq(buildInfoKeys := Seq[BuildInfoKey](name, version, git.gitHeadCommit),
    buildInfoPackage := "ml.combust.mleap",
    buildInfoObject := "BuildValues",
    buildInfoOptions += BuildInfoOption.ToJson)
}