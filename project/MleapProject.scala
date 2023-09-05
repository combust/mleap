package ml.combust.mleap

import sbt.Keys._
import sbt._

object MleapProject {


  var rootSettings = Release.settings ++
    Common.buildSettings ++
    Common.sonatypeSettings ++
    Seq(publishArtifact := false)

  lazy val root = Project(
    id = "mleap",
    base = file(".")
  ).aggregate(baseProject,
    tensor,
    tensorflow,
    bundleMl,
    bundleHdfs,
    core,
    runtime,
    xgboostRuntime,
    xgboostSpark,
    avro,
    sparkBase,
    sparkTestkit,
    spark,
    sparkExtension,
    executor,
    executorTestKit,
    grpc,
    grpcServer,
    repositoryS3,
    springBootServing,
    serving,
    databricksRuntime)
  .settings(rootSettings)

  lazy val baseProject = Project(
    id = "mleap-base",
    base = file("mleap-base")
  )

  lazy val tensor = Project(
    id = "mleap-tensor",
    base = file("mleap-tensor")
  ).dependsOn(baseProject)

  lazy val bundleMl = Project(
    id = "bundle-ml",
    base = file("bundle-ml")
  ).dependsOn(baseProject, tensor)

  lazy val bundleHdfs = Project(
    id = "bundle-hdfs",
    base = file("bundle-hdfs")
  ).dependsOn(bundleMl)

  lazy val core = Project(
    id = "mleap-core",
    base = file("mleap-core")
  ).dependsOn(baseProject, tensor)

  lazy val runtime = Project(
    id = "mleap-runtime",
    base = file("mleap-runtime")
  ).dependsOn(core, bundleMl)

  lazy val avro = Project(
    id = "mleap-avro",
    base = file("mleap-avro")
  ).dependsOn(runtime)

  lazy val sparkBase = Project(
    id = "mleap-spark-base",
    base = file("mleap-spark-base")
  ).dependsOn(runtime, bundleHdfs)

  lazy val sparkTestkit = Project(
    id = "mleap-spark-testkit",
    base = file("mleap-spark-testkit")
  ).dependsOn(sparkBase)

  lazy val spark = Project(
    id = "mleap-spark",
    base = file("mleap-spark")
  ).dependsOn(sparkBase, sparkTestkit % "test")

  lazy val sparkExtension = Project(
    id = "mleap-spark-extension",
    base = file("mleap-spark-extension")
  ).dependsOn(spark, sparkTestkit % "test")

  lazy val tensorflow = Project(
    id = "mleap-tensorflow",
    base = file("mleap-tensorflow")
  ).dependsOn(runtime)

  lazy val xgboostRuntimeSettings = inConfig(Test)(Defaults.testSettings) ++ Seq(
    // xgboost has trouble with multi-threading so avoid parallel executions.
     Test / parallelExecution := false,
  )
  lazy val xgboostRuntime = Project(
    id = "mleap-xgboost-runtime",
    base = file("mleap-xgboost-runtime")
  ).dependsOn(runtime, sparkTestkit % "test")
  .settings(xgboostRuntimeSettings)

  lazy val xgboostSpark = Project(
    id = "mleap-xgboost-spark",
    base = file("mleap-xgboost-spark")
  ).dependsOn(
    sparkBase % "provided",
      xgboostRuntime % "test",
      spark % "test",
      sparkTestkit % "test"
    )

  lazy val serving = Project(
    id = "mleap-serving",
    base = file("mleap-serving")
  ).dependsOn(springBootServing, grpcServer)

  lazy val executor = Project(
    id = "mleap-executor",
    base = file("mleap-executor")
  ).dependsOn(runtime)

  lazy val executorTestKit = Project(
    id = "mleap-executor-testkit",
    base = file("mleap-executor-testkit")
  ).dependsOn(executor)

  private val executorTestSettings = inConfig(Test)(Defaults.testSettings) ++ Seq(
    // Supports classloading "magic" from the sbt.
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  lazy val executorTests = Project(
    id = "mleap-executor-tests",
    base = file("mleap-executor-tests")
  ).dependsOn(executor, executorTestKit % "test")
    .settings(executorTestSettings)

  lazy val grpc = Project(
    id = "mleap-grpc",
    base = file("mleap-grpc")
  ).dependsOn(`executor`)

  private val grpcServerSettings = inConfig(Test)(Defaults.testSettings) ++ Seq(
    // Supports classloading "magic" from the sbt.
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  lazy val grpcServer = Project(
    id = "mleap-grpc-server",
    base = file("mleap-grpc-server")
  ).dependsOn(grpc, executorTestKit % "test").settings(grpcServerSettings)

  lazy val repositoryS3 = Project(
    id = "mleap-repository-s3",
    base = file("mleap-repository-s3")
  ).dependsOn(executor)

  private val springBootSettings = inConfig(Test)(Defaults.testSettings) ++ Seq(
    // spring-boot: avoiding tomcat's java.lang.Error: factory already defined
    // refer to https://github.com/spring-projects/spring-boot/issues/21535
    Test / fork := true,
  )

  lazy val springBootServing = Project(
    id = "mleap-spring-boot",
    base = file("mleap-spring-boot")
  ).dependsOn(executor).settings(springBootSettings)

  lazy val benchmark = Project(
    id = "mleap-benchmark",
    base = file("mleap-benchmark")
  ).dependsOn(runtime, spark, avro)

  // Create underlying fat jar project as per: https://github.com/sbt/sbt-assembly#q-despite-the-concerned-friends-i-still-want-publish-fat-jars-what-advice-do-you-have
  lazy val databricksRuntimeFat = Project(
    id = "mleap-databricks-runtime-fat",
    base = file("mleap-databricks-runtime-fat")
  ).dependsOn(baseProject,
      tensor,
      core,
      runtime,
      bundleMl,
      spark,
      sparkExtension,
      tensorflow,
      xgboostRuntime,
      xgboostSpark)
  .settings(excludeDependencies ++= Seq(
    ExclusionRule("org.tensorflow"),
    ExclusionRule("org.apache.spark"),
    ExclusionRule("ml.dmlc")
  ))

  lazy val databricksRuntime = Project(
    id = "mleap-databricks-runtime",
    base = file("mleap-databricks-runtime")
  )

  lazy val databricksRuntimeTestkit = Project(
    id = "mleap-databricks-runtime-testkit",
    base = file("mleap-databricks-runtime-testkit")
  ).dependsOn(spark % "provided",
      sparkExtension % "provided",
      xgboostSpark % "provided",
      tensorflow % "provided")
}
