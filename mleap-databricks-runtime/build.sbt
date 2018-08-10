import ml.combust.mleap.{Common, MleapProject}

resolvers += Resolver.mavenLocal

Common.defaultMleapSettings

packageBin in Compile := (assembly in (MleapProject.databricksRuntimeFat, Compile)).value
