import ml.combust.mleap.{Common, MleapProject}

Common.defaultMleapSettings

packageBin in Compile := (assembly in (MleapProject.databricksRuntimeFat, Compile)).value

publishMavenStyle := true
