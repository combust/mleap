package ml.combust.mleap.serving

import java.io.File
import java.net.URI
import java.util.UUID

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.DefaultLeapFrame
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import resource.managed
import ml.combust.mleap.runtime.frame.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

object TestUtil {
  val baseDir = new File("/tmp/mleap-serving")
  TestUtil.delete(baseDir)
  baseDir.mkdirs()

  def delete(file: File): Array[(String, Boolean)] = {
    Option(file.listFiles).map(_.flatMap(f => delete(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
  }

  def getLeapFrame : DefaultLeapFrame = {
    DefaultLeapFrame(StructType(
      StructField("first_double", ScalarType.Double),
      StructField("second_double", ScalarType.Double),
      StructField("third_double", ScalarType.Double)).get,
      Seq(Row(5d, 1.0, 4.0), Row(2.0, 2.0, 4.0),
        Row(3.0, 2.0, 5.0)))
  }

  def serializeModelInJsonFormatToZipFile : String = {
    val bundleName = UUID.randomUUID().toString

    val vectorAssembler = VectorAssembler(
      shape = NodeShape().withInput("input0", "feature1").
        withInput("input1", "feature2").
        withInput("input2", "feature3").
        withStandardOutput("features"),
      model = VectorAssemblerModel(Seq(TensorShape(3), ScalarShape(), ScalarShape())))


    val model = VectorAssemblerModel(Seq(ScalarShape(), ScalarShape(), ScalarShape()))
    val featureAssembler = VectorAssembler(
      shape = NodeShape().withInput("input0", "first_double").
      withInput("input1", "second_double").
      withInput("input2", "third_double").
      withStandardOutput("features"),
      model = model)
    val linearRegression = LinearRegression(shape = NodeShape.regression(),
      model = LinearRegressionModel(Vectors.dense(2.0, 1.0, 2.0), 5d))
    val pipeline = Pipeline("pipeline", NodeShape(),
      PipelineModel(Seq(featureAssembler, linearRegression)))

    val uri = new URI(s"jar:file:${TestUtil.baseDir}/$bundleName.json.zip")
    for (file <- managed(BundleFile(uri))) {
      pipeline.writeBundle.name("bundle")
        .format(SerializationFormat.Json)
        .save(file)
    }

    s"$baseDir/$bundleName.json.zip"
  }
}