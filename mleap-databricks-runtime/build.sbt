import ml.combust.mleap.{Common, MleapProject}

Common.defaultMleapSettings

Compile / packageBin := (MleapProject.databricksRuntimeFat / Compile / assembly).value

publishMavenStyle := true
