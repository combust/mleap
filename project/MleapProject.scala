package ml.combust.mleap

import sbt.Keys._
import sbt._

object MleapProject {
  lazy val aggregatedProjects: Seq[ProjectReference] = Seq(baseProject,
    tensor,
    tensorflow,
    bundleMl,
    bundleHdfs,
    core,
    runtime,
    avro,
    sparkBase,
    sparkTestkit,
    spark,
    sparkExtension,
    xgboostRuntime,
    xgboostSpark,
    tensorflow,
    executor,
    executorTestKit,
    grpc,
    grpcServer,
    repositoryS3,
    springBootServing,
    serving,
    databricksRuntime)

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

  lazy val bundleHdfs = Project(
    id = "bundle-hdfs",
    base = file("bundle-hdfs"),
    dependencies = Seq(bundleMl)
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
    dependencies = Seq(runtime, bundleHdfs)
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

  lazy val xgboostRuntime = Project(
    id = "mleap-xgboost-runtime",
    base = file("mleap-xgboost-runtime"),
    dependencies = Seq(runtime)
  )

  lazy val xgboostSpark = Project(
    id = "mleap-xgboost-spark",
    base = file("mleap-xgboost-spark"),
    dependencies = Seq(sparkBase % "provided",
      xgboostRuntime % "test",
      spark % "test",
      sparkTestkit % "test")
  )

  lazy val serving = Project(
    id = "mleap-serving",
    base = file("mleap-serving"),
    dependencies = Seq(springBootServing, grpcServer)
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

  lazy val repositoryS3 = Project(
    id = "mleap-repository-s3",
    base = file("mleap-repository-s3"),
    dependencies = Seq(executor)
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

  // Create underlying fat jar project as per: https://github.com/sbt/sbt-assembly#q-despite-the-concerned-friends-i-still-want-publish-fat-jars-what-advice-do-you-have
  lazy val databricksRuntimeFat = Project(
    id = "mleap-databricks-runtime-fat",
    base = file("mleap-databricks-runtime-fat"),
    dependencies = Seq(baseProject,
      tensor,
      core,
      runtime,
      bundleMl,
      spark,
      sparkExtension,
      tensorflow)
  ).settings(excludeDependencies ++= Seq(
    SbtExclusionRule("org.tensorflow"),
    SbtExclusionRule("org.apache.spark")
  ))

  lazy val databricksRuntime = Project(
    id = "mleap-databricks-runtime",
    base = file("mleap-databricks-runtime"),
    dependencies = Seq()
  )

  lazy val databricksRuntimeTestkit = Project(
    id = "mleap-databricks-runtime-testkit",
    base = file("mleap-databricks-runtime-testkit"),
    dependencies = Seq(spark % "provided",
      sparkExtension % "provided",
      tensorflow % "provided")
  )
}