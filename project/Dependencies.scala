package ml.combust.mleap

import sbt._
import Keys._

object Dependencies {
  import DependencyHelpers._

  val sparkVersion = "2.1.0"
  val scalaTestVersion = "3.0.0"

  object Compile {
    val sparkMllibLocal = "org.apache.spark" %% "spark-mllib-local" % sparkVersion
    val spark = Seq("org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion)
    val avroDep = "org.apache.avro" % "avro" % "1.8.1"
    val sprayJson = "io.spray" %% "spray-json" % "1.3.2"
    val arm = "com.jsuereth" %% "scala-arm" % "2.0-RC1"
    val config = "com.typesafe" % "config" % "1.3.0"
    val scalaReflect = ScalaVersionDependentModuleID.versioned("org.scala-lang" % "scala-reflect" % _)
    val sparkAvro = "com.databricks" %% "spark-avro" % "3.0.1"
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
    val jTransform = "edu.emory.mathcs" % "JTransforms" % "2.4"
    val tensorflowCppPresets = "org.bytedeco.javacpp-presets" % "tensorflow" % "0.11.0-1.3"
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
  }

  object Provided {
    val spark = Compile.spark.map(_ % "provided")
  }

  import Compile._
  val l = libraryDependencies

  val bundleMl = l ++= Seq(sprayJson, arm, config, Test.scalaTest)

  val base = l ++= Seq()

  val core = l ++= Seq(sparkMllibLocal, jTransform, Test.scalaTest)

  def runtime(scalaVersion: SettingKey[String]) = l ++= (Seq(Test.scalaTest) ++ scalaReflect.modules(scalaVersion.value))

  val sparkBase = l ++= Provided.spark ++ Seq(Test.scalaTest)

  val sparkTestkit = l ++= Provided.spark ++ Seq(sparkAvro, scalaTest)

  val spark = l ++= Provided.spark

  val sparkExtension = l ++= Provided.spark ++ Seq(Test.scalaTest)

  val avro = l ++= Seq(avroDep)

  val tensorflow = l ++= Seq(tensorflowCppPresets)

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