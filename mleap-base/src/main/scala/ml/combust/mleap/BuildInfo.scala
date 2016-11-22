package ml.combust.mleap

/**
  * Created by hollinwilkins on 11/2/16.
  */
case class BuildInfo(name: String = BuildValues.name,
                     version: String = BuildValues.version,
                     gitSha: String = BuildValues.gitHeadCommit.get)
