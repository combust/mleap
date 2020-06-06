import ml.combust.mleap.{Common, Dependencies}

Common.defaultMleapSettings
Common.noPublishSettings

Dependencies.runtime(scalaVersion)
Dependencies.spark
Dependencies.tensorflow

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-core" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-core" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-avro" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-avro" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-runtime" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-runtime" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark-extension" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark-extension" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-tensor" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-tensor" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-tensorflow" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-tensorflow" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-xgboost-runtime" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-xgboost-runtime" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-xgboost-spark" / "src" / "test" / "scala"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-xgboost-spark" / "src" / "test" / "resources"

unmanagedSourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark-testkit" / "src" / "main"
unmanagedResourceDirectories in Test += baseDirectory.value / ".." / "mleap-spark-testkit" / "src" / "main" / "resources"
