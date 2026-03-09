import ml.combust.mleap.{Dependencies, Common, BuildInfo}

enablePlugins(BuildInfoPlugin, GitPlugin)

Common.defaultMleapSettings
Dependencies.base
BuildInfo.settings
