import ml.combust.mleap.{Common, Dependencies, Protobuf}

enablePlugins(DockerPlugin, JavaAppPackaging)

Dependencies.springBootServing
Common.defaultMleapSettings
Protobuf.springBootSettings
DockerConfig.baseSettings