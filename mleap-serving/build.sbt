import ml.combust.mleap.{Dependencies, Common}

enablePlugins(DockerPlugin, JavaAppPackaging)

Common.defaultMleapServingSettings
Dependencies.serving
DockerConfig.baseSettings
