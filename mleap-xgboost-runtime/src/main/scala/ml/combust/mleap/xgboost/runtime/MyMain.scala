package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.tensor.{DenseTensor, Tensor}

object MyMain {

  def main(args: Array[String]): Unit = {

    import ml.combust.bundle.BundleFile
    import ml.combust.mleap.runtime.MleapSupport._
    import resource._

    import java.nio.file.Paths
    val modelArtifactName = "pixar_movie_mleap_bundle_old.zip"
    val artifactLocalFile = Paths.get(this.asInstanceOf[Any].getClass.getClassLoader.getResource(modelArtifactName).toURI).toFile

    // load the Spark pipeline we saved in the previous section
    val bundle = (for (bundleFile <- managed(BundleFile(s"jar:file:${artifactLocalFile.getAbsolutePath}"))) yield {
      bundleFile.loadMleapBundle().get
    }).tried.get

    // create a simple LeapFrame to transform
    import ml.combust.mleap.core.types._
    import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

    // MLeap makes extensive use of monadic types like Try
    val schema = StructType(
      StructField("features", TensorType(BasicType.Double, Seq(2)))
    ).get
//      StructField("long_feature", ScalarType.Long),
//      StructField("double_feature", ScalarType.Double)).get

    val data = Seq(
      Row(
//        "features",
//        TensorType(Double, Seq(3))
        Tensor.denseVector(Array(0.2, 0.7))
//        TensorType(Double, Some(Array(0.1, 0.2)), false)
      )
    )
//    val data = Seq(Row("hello", 65L), Row("MLeap", 0.2))
    val frame = DefaultLeapFrame(schema, data)

    // transform the dataframe using our pipeline
    val mleapPipeline = bundle.root
    val frame2 = mleapPipeline.transform(frame).get
    println(frame2.dataset)


  }
}
