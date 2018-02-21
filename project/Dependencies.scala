package ml.combust.mleap

import sbt._
import Keys._

object Dependencies {
  val sparkVersion = "2.2.2"
  val scalaTestVersion = "3.0.0"
  val tensorflowVersion = "1.4.0"
  val akkaVersion = "2.4.16"
  val akkaHttpVersion = "10.0.3"

  object Compile {
    val sparkMllibLocal = "org.apache.spark" %% "spark-mllib-local" % sparkVersion excludeAll(ExclusionRule(organization = "org.scalatest"))
    val breeze = "org.scalanlp" %% "breeze" % "1.0-RC2"
    val netlib = "com.github.fommil.netlib" % "all" % "1.1.2"
    val spark = Seq("org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion)
    val avroDep = "org.apache.avro" % "avro" % "1.8.1"
    val sprayJson = "io.spray" %% "spray-json" % "1.3.2"
    val arm = "com.jsuereth" %% "scala-arm" % "2.0"
    val config = "com.typesafe" % "config" % "1.3.0"
    val scalaReflect = "org.scala-lang" % "scala-reflect"
    val sparkAvro = "com.databricks" %% "spark-avro" % "3.0.1"
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
    val jTransform = "com.github.rwl" % "jtransforms" % "2.4.0" exclude("junit", "junit")
    val tensorflowDep = "org.tensorflow" % "libtensorflow" % tensorflowVersion
    val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
    val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
    val scalameter = "com.storm-enroute" %% "scalameter" % "0.8.2"
    val scopt = "com.github.scopt" %% "scopt" % "3.5.0"
    val jafama = "net.jafama" % "jafama" % "2.1.0"
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val akkaHttpTestkit =  "com.typesafe.akka" % "akka-http-testkit_2.11" % akkaHttpVersion % "test"
  }

  object Provided {
    val spark = Compile.spark.map(_.excludeAll(ExclusionRule(organization = "org.scalatest"))).map(_ % "provided")
    val xgboostSparkDep = "ml.dmlc" % "xgboost4j-spark" % "0.7" % "provided"
  }

  import Compile._
  val l = libraryDependencies

  val tensor = l ++= Seq(sprayJson, Test.scalaTest)

  val bundleMl = l ++= Seq(arm, config, sprayJson, Test.scalaTest)

  val base = l ++= Seq()

  val core = l ++= Seq(breeze, netlib, jTransform, Test.scalaTest)

  val runtime = l ++= Seq(Test.scalaTest, scalaReflect % scalaVersion.value)

  val sparkBase = l ++= Provided.spark ++ Seq(Test.scalaTest)

  val sparkTestkit = l ++= Provided.spark ++ Seq(sparkAvro, scalaTest)

  val spark = l ++= Provided.spark

  val sparkExtension = l ++= Provided.spark ++ Seq(Test.scalaTest)

  val avro = l ++= Seq(avroDep, Test.scalaTest)

  val tensorflow = l ++= Seq(tensorflowDep, Test.scalaTest)

  val xgboostJava = l ++= Seq(jafama)

  val xgboostSpark = l ++= Seq(Provided.xgboostSparkDep) ++ Provided.spark

  val serving = l ++= Seq(akkaHttp, akkaHttpSprayJson, config, Test.scalaTest, Test.akkaHttpTestkit)

  val benchmark = l ++= Seq(scalameter, scopt, sparkAvro) ++ Compile.spark
}
