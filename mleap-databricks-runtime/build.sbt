import ml.combust.mleap.{Common, MleapProject}

Common.defaultMleapSettings

packageBin in Compile := (assembly in (MleapProject.databricksRuntimeFat, Compile)).value

publishMavenStyle := true

publishTo := {
    Some("Databricks Repository on S3" at "s3://s3.amazonaws.com/databricks-mvn/release")
}
