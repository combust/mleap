import ml.combust.mleap.{Common, Dependencies}

Common.defaultMleapSettings
Common.noPublishSettings

Dependencies.runtime(scalaVersion)
Dependencies.spark
Dependencies.tensorflow

lazy val genTestCodeTask = TaskKey[Unit]("removeCacheFile", "Deletes a cache file")
genTestCodeTask := {
  import sys.process._
  Seq("mleap-databricks-runtime-fat-tests/genTestCode.sh")!
}

(test in Test) <<= (test in Test).dependsOn(genTestCodeTask)

//compile in Compile <<= (compile in Compile).dependsOn(genTestCodeTask)
