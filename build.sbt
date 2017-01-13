import ml.combust.mleap.{Release, Common}

lazy val aggregatedProjects: Seq[ProjectReference] = {
  val base: Seq[ProjectReference] = Seq(baseProject,
    bundleMl,
    core,
    runtime,
    avro,
    sparkBase,
    sparkTestkit,
    spark,
    sparkExtension)

  sys.props.get("mleap.tensorflow.compile") match {
    case Some("true") => base :+ (tensorflow: ProjectReference)
    case _ => base
  }
}

lazy val rootSettings = Release.settings ++ Common.buildSettings ++ Seq(publishArtifact := false)

lazy val root = Project(
  id = "mleap",
  base = file("."),
  aggregate = aggregatedProjects
).settings(rootSettings)

lazy val baseProject = Project(
  id = "mleap-base",
  base = file("mleap-base")
)

lazy val bundleMl = Project(
  id = "bundle-ml",
  base = file("bundle-ml"),
  dependencies = Seq(baseProject)
)

lazy val core = Project(
  id = "mleap-core",
  base = file("mleap-core"),
  dependencies = Seq(baseProject)
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