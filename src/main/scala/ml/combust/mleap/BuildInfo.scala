package ml.combust.mleap

/** Class containing build info for this Bundle.ML release.
  *
  * @param version version number set by SBT
  * @param sha GIT sha of this version
  * @param scalaVersion Scala version used for compilation
  */
case class BuildInfo(version: Option[String] = Some(BuildInfoKeys.version),
                     sha: Option[String] = BuildInfoKeys.gitHeadCommit,
                     scalaVersion: Option[String] = Some(BuildInfoKeys.scalaVersion))
