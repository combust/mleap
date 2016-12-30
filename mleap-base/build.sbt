import ml.combust.mleap.{Dependencies, Common, BuildInfo}

enablePlugins(BuildInfoPlugin, GitVersioning)

Common.defaultMleapSettings
Dependencies.base
BuildInfo.settings
