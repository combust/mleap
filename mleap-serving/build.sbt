import ml.combust.mleap.{Dependencies, Common}

enablePlugins(DockerPlugin, JavaAppPackaging)

Common.defaultMleapSettings
Dependencies.serving
DockerConfig.baseSettings
