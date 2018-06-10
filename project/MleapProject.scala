package ml.combust.mleap

import sbt.Keys._
import sbt._

object MleapProject {
  lazy val aggregatedProjects: Seq[ProjectReference] = Seq(baseProject,
      tensor,
      tensorflow,
      bundleMl,
      core,
      runtime,
      avro,
      sparkBase,
      sparkTestkit,
      spark,
      sparkExtension,
      xgboostJava,
      tensorflow)

  var rootSettings = Release.settings ++
    Common.buildSettings ++
    Common.sonatypeSettings ++
    Seq(publishArtifact := false)

  lazy val root = Project(
    id = "mleap",
    base = file("."),
    aggregate = aggregatedProjects
  ).settings(rootSettings)

  lazy val baseProject = Project(
    id = "mleap-base",
    base = file("mleap-base")
  )

  lazy val tensor = Project(
    id = "mleap-tensor",
    base = file("mleap-tensor"),
    dependencies = Seq(baseProject)
  )

  lazy val bundleMl = Project(
    id = "bundle-ml",
    base = file("bundle-ml"),
    dependencies = Seq(baseProject, tensor)
  )

  lazy val core = Project(
    id = "mleap-core",
    base = file("mleap-core"),
    dependencies = Seq(baseProject, tensor)
  )

  lazy val runtime = Project(
    id = "mleap-runtime",
    base = file("mleap-runtime"),
    dependencies = Seq(core, bundleMl)
  )

  lazy val avro = Project(
    id = "mleap-avro",
    base = file("mleap-avro"),
    dependencies = Seq(runtime)
  )

  lazy val sparkBase = Project(
    id = "mleap-spark-base",
    base = file("mleap-spark-base"),
    dependencies = Seq(runtime)
  )

  lazy val sparkTestkit = Project(
    id = "mleap-spark-testkit",
    base = file("mleap-spark-testkit"),
    dependencies = Seq(sparkBase)
  )

  lazy val spark = Project(
    id = "mleap-spark",
    base = file("mleap-spark"),
    dependencies = Seq(sparkBase, sparkTestkit % "test")
  )

  lazy val sparkExtension = Project(
    id = "mleap-spark-extension",
    base = file("mleap-spark-extension"),
    dependencies = Seq(spark, sparkTestkit % "test")
  )

  lazy val tensorflow = Project(
    id = "mleap-tensorflow",
    base = file("mleap-tensorflow"),
    dependencies = Seq(runtime)
  )

  lazy val xgboostJava = Project(
    id = "mleap-xgboost-java",
    base = file("mleap-xgboost-java"),
    dependencies = Seq(runtime)
  )

  lazy val xgboostSpark = Project(
    id = "mleap-xgboost-spark",
    base = file("mleap-xgboost-spark"),
    dependencies = Seq(sparkBase % "provided",
      xgboostJava % "test",
      spark % "test",
      sparkTestkit % "test")
  )

  lazy val serving = Project(
    id = "mleap-serving",
    base = file("mleap-serving"),
    dependencies = Seq(runtime, avro, xgboostJava)
  )

  lazy val executor = Project(
    id = "mleap-executor",
    base = file("mleap-executor"),
    dependencies = Seq(runtime)
  )

  lazy val executorTestKit = Project(
    id = "mleap-executor-testkit",
    base = file("mleap-executor-testkit"),
    dependencies = Seq(executor)
  )

  lazy val executorTests = Project(
    id = "mleap-executor-tests",
    base = file("mleap-executor-tests"),
    dependencies = Seq(executor, executorTestKit % "test")
  )

  lazy val grpc = Project(
    id = "mleap-grpc",
    base = file("mleap-grpc"),
    dependencies = Seq(`executor`)
  )

  lazy val grpcServer = Project(
    id = "mleap-grpc-server",
    base = file("mleap-grpc-server"),
    dependencies = Seq(grpc, executorTestKit % "test")
  )

  lazy val springBootServing = Project(
    id = "mleap-spring-boot",
    base = file("mleap-spring-boot"),
    dependencies = Seq(`executor`)
  )

  lazy val benchmark = Project(
    id = "mleap-benchmark",
    base = file("mleap-benchmark"),
    dependencies = Seq(runtime, spark, avro)
  )
}