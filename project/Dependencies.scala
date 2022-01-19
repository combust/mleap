package ml.combust.mleap

import sbt._
import Keys._

object Dependencies {
  import DependencyHelpers._

  val sparkVersion = "3.2.0"
  val scalaTestVersion = "3.0.8"
  val junitVersion = "5.8.2"
  val akkaVersion = "2.6.14"
  val akkaHttpVersion = "10.2.4"
  val springBootVersion = "2.6.2"
  lazy val logbackVersion = "1.2.3"
  lazy val loggingVersion = "3.9.0"
  lazy val slf4jVersion = "1.7.30"
  lazy val awsSdkVersion = "1.11.1033"
  val tensorflowJavaVersion = "0.3.1" // Match Tensorflow 2.4.1 https://github.com/tensorflow/java/#tensorflow-version-support
  val xgboostVersion = "1.5.1"
  val breezeVersion = "1.0"
  val hadoopVersion = "2.7.4" // matches spark version
  val platforms = "windows-x86_64,linux-x86_64,macosx-x86_64"
  val tensorflowPlatforms : Array[String] =  sys.env.getOrElse("TENSORFLOW_PLATFORMS", platforms).split(",")

  object Compile {
    val sparkMllibLocal = "org.apache.spark" %% "spark-mllib-local" % sparkVersion excludeAll(ExclusionRule(organization = "org.scalatest"))
    val spark = Seq("org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion,
      "org.apache.spark" %% "spark-avro" % sparkVersion
    )
    val avroDep = "org.apache.avro" % "avro" % "1.8.1"
    val sprayJson = "io.spray" %% "spray-json" % "1.3.2"
    val arm = "com.jsuereth" %% "scala-arm" % "2.0"
    val config = "com.typesafe" % "config" % "1.3.0"
    val scalaReflect = ScalaVersionDependentModuleID.versioned("org.scala-lang" % "scala-reflect" % _)
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
    val jTransform = "com.github.rwl" % "jtransforms" % "2.4.0" exclude("junit", "junit")
    val commonsIo = "commons-io" % "commons-io" % "2.5"
    var tensorflowCoreApi = "org.tensorflow" % "tensorflow-core-api" % tensorflowJavaVersion
    (Seq("") ++ tensorflowPlatforms).foreach(platform => tensorflowCoreApi = tensorflowCoreApi classifier platform)
    val tensorflowDeps = Seq(tensorflowCoreApi)
    val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

    val akkaStream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
    val akkaHttp = "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
    val akkaHttpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
    val scalameter = "com.storm-enroute" %% "scalameter" % "0.8.2"
    val scopt = "com.github.scopt" %% "scopt" % "3.5.0"

    val springBoot = "org.springframework.boot" % "spring-boot-starter-web" % springBootVersion
    val springBootActuator = "org.springframework.boot" % "spring-boot-starter-actuator" % springBootVersion

    val commonsLang = "org.apache.commons" % "commons-lang3" % "3.7"
    val scalaPb = Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
      "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
      "com.thesamet.scalapb" %% "scalapb-json4s" % scalapb.compiler.Version.scalapbVersion
    )

    val awsS3 = "com.amazonaws" % "aws-java-sdk-s3" % awsSdkVersion

    lazy val logging = Seq(
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % loggingVersion
    )

    val breeze = "org.scalanlp" %% "breeze" % breezeVersion

    val xgboostDep = "ml.dmlc" %% "xgboost4j" % xgboostVersion
    val xgboostSparkDep = "ml.dmlc" %% "xgboost4j-spark" % xgboostVersion
    val xgboostPredictorDep = "ai.h2o" % "xgboost-predictor" % "0.3.18" exclude("com.esotericsoftware.kryo", "kryo")

    val hadoop = "org.apache.hadoop" % "hadoop-client" % hadoopVersion

    val slf4jDep = "org.slf4j" % "slf4j-log4j12" % slf4jVersion
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
    val akkaHttpTestkit =  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test"
    val akkaTestKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    val springBootTest = "org.springframework.boot" % "spring-boot-starter-test" % springBootVersion % "test"
    val akkaStreamTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % "test"
    val junit = "org.junit.jupiter" % "junit-jupiter" % junitVersion % "test"
    val spark = Compile.spark.map(_ % "test")
    val sparkTest = Compile.spark.map(_ % "test" classifier "tests")
  }

  object Provided {
    val spark = Compile.spark.map(_.excludeAll(ExclusionRule(organization = "org.scalatest"))).map(_ % "provided")
    val sparkTestLib = spark.map(_ classifier "tests")
    val hadoop = Compile.hadoop % "provided"
  }

  import Compile._
  val l = libraryDependencies

  val tensor = l ++= Seq(sprayJson, Test.scalaTest)

  val bundleMl = l ++= Seq(arm, config, sprayJson, Test.scalaTest)

  val bundleHdfs = l ++= Seq(Provided.hadoop, Test.scalaTest)

  val base = l ++= Seq()

  val core = l ++= Seq(sparkMllibLocal, jTransform, breeze, Test.scalaTest) ++ Test.sparkTest

  def runtime(scalaVersion: SettingKey[String]) = l ++= (Seq(Test.scalaTest, Test.junit, commonsIo) ++ scalaReflect.modules(scalaVersion.value))

  val sparkBase = l ++= Provided.spark ++ Seq(Test.scalaTest)

  val sparkTestkit = l ++= Provided.spark ++ Provided.sparkTestLib ++ Seq(scalaTest)

  val spark = l ++= Provided.spark ++ Test.sparkTest

  val sparkExtension = l ++= Provided.spark ++ Seq(Compile.slf4jDep) ++ Seq(Test.scalaTest) ++ Test.sparkTest

  val avro = l ++= Seq(avroDep, Test.scalaTest)

  val tensorflow = l ++= tensorflowDeps ++ Seq(Test.scalaTest)

  val xgboostRuntime = l ++= Seq(xgboostDep) ++ Seq(xgboostPredictorDep) ++ Test.spark ++ Test.sparkTest ++ Seq(Test.scalaTest)

  val xgboostSpark = l ++= Seq(xgboostSparkDep) ++ Provided.spark ++ Test.spark ++ Test.sparkTest

  val serving = l ++= Seq(akkaHttp, akkaHttpSprayJson, config, Test.scalaTest, Test.akkaHttpTestkit)

  val executor = l ++= Seq(akkaStream, config, Test.scalaTest, Test.akkaTestKit) ++ logging

  val executorTestKit = l ++= Seq(scalaTest, akkaTestKit, akkaStreamTestKit)

  val grpcServer = l ++= Seq(scopt) ++ Seq(Test.scalaTest, Test.akkaStreamTestKit)

  val repositoryS3 = l ++= Seq(awsS3)

  val grpc = l ++= Seq(
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion) ++ scalaPb

  val springBootServing = l ++= Seq(springBoot, springBootActuator, commonsLang, Test.scalaTest, Test.springBootTest) ++ scalaPb

  val benchmark = l ++= Seq(scalameter, scopt) ++ Compile.spark

  val databricksRuntimeTestkit = l ++= Provided.spark

  object DependencyHelpers {
    case class ScalaVersionDependentModuleID(modules: String => Seq[ModuleID]) {
      def %(config: String): ScalaVersionDependentModuleID =
        ScalaVersionDependentModuleID(version => modules(version).map(_ % config))
    }
    object ScalaVersionDependentModuleID {
      implicit def liftConstantModule(mod: ModuleID): ScalaVersionDependentModuleID = versioned(_ => mod)

      def versioned(f: String => ModuleID): ScalaVersionDependentModuleID = ScalaVersionDependentModuleID(v => Seq(f(v)))
      def fromPF(f: PartialFunction[String, ModuleID]): ScalaVersionDependentModuleID =
        ScalaVersionDependentModuleID(version => if (f.isDefinedAt(version)) Seq(f(version)) else Nil)
    }

    /**
      * Use this as a dependency setting if the dependencies contain both static and Scala-version
      * dependent entries.
      */
    def versionDependentDeps(modules: ScalaVersionDependentModuleID*): Def.Setting[Seq[ModuleID]] =
      libraryDependencies <++= scalaVersion(version => modules.flatMap(m => m.modules(version)))

    val ScalaVersion = """\d\.\d+\.\d+(?:-(?:M|RC)\d+)?""".r
    val nominalScalaVersion: String => String = {
      // matches:
      // 2.12.0-M1
      // 2.12.0-RC1
      // 2.12.0
      case version @ ScalaVersion() => version
      // transforms 2.12.0-custom-version to 2.12.0
      case version => version.takeWhile(_ != '-')
    }
  }
}
