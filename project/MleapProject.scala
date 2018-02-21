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
      xgboostJava)

  var rootSettings = Release.settings ++
    Common.buildSettings ++
    Common.sonatypeSettings ++
    Seq(publishArtifact := false)

  if(!sys.env.contains("TENSORFLOW_JNI")) {
    // skip tests because of JNI library requirement
    rootSettings ++= Seq(test in tensorflow in Test := false)
  }

  lazy val root = Project(
    id = "mleap",
    base = file(".")
  ).settings(rootSettings).
    aggregate(aggregatedProjects: _*)

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

  lazy val core = Project(
    id = "mleap-core",
    base = file("mleap-core")
  ).dependsOn(baseProject)

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
  ).dependsOn(runtime)

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

  lazy val xgboostJava = Project(
    id = "mleap-xgboost-java",
    base = file("mleap-xgboost-java")
  ).dependsOn(runtime)

  lazy val xgboostSpark = Project(
    id = "mleap-xgboost-spark",
    base = file("mleap-xgboost-spark")
  ).dependsOn(
    sparkBase % "provided",
    xgboostJava % "test",
    spark % "test",
    sparkTestkit % "test"
  )

  lazy val serving = Project(
    id = "mleap-serving",
    base = file("mleap-serving")
  ).dependsOn(runtime, avro, xgboostJava)

  lazy val benchmark = Project(
    id = "mleap-benchmark",
    base = file("mleap-benchmark")
  ).dependsOn(runtime, spark, avro)
}