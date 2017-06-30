package ml.combust.mleap.serving

import java.io.File
import java.net.URI
import java.util.UUID

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.core.types.{DoubleType, StructField, StructType}
import ml.combust.mleap.runtime.transformer.Pipeline
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.mleap.runtime.types.StructType
import ml.combust.mleap.runtime.{DefaultLeapFrame, LeapFrame, LocalDataset, Row}
import org.apache.spark.ml.linalg.Vectors
import resource.managed
import ml.combust.mleap.runtime.MleapSupport._

object TestUtil {
  val baseDir = new File("/tmp/mleap-serving")
  TestUtil.delete(baseDir)
  baseDir.mkdirs()

  def delete(file: File): Array[(String, Boolean)] = {
    Option(file.listFiles).map(_.flatMap(f => delete(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
  }

  def getLeapFrame : DefaultLeapFrame = {
    LeapFrame(StructType(
      StructField("first_double", DoubleType()),
      StructField("second_double", DoubleType()),
      StructField("third_double", DoubleType())).get,
      LocalDataset(Row(5d, 1.0, 4.0), Row(2.0, 2.0, 4.0),
        Row(3.0, 2.0, 5.0)))
  }

  def serializeModelInJsonFormatToZipFile : String = {
    val bundleName = UUID.randomUUID().toString

    val featureAssembler = VectorAssembler(inputCols = Array("first_double", "second_double", "third_double"),
      inputDataTypes = Some(Array(DoubleType(), DoubleType(), DoubleType())),
      outputCol = "features")
    val linearRegression = LinearRegression(featuresCol = "features", predictionCol = "prediction",
      model = LinearRegressionModel(Vectors.dense(2.0, 1.0, 2.0), 5d))
    val pipeline = Pipeline("pipeline", Seq(featureAssembler, linearRegression))

    val uri = new URI(s"jar:file:${TestUtil.baseDir}/${bundleName}.json.zip")
    for (file <- managed(BundleFile(uri))) {
      pipeline.writeBundle.name("bundle")
        .format(SerializationFormat.Json)
        .save(file)
    }

    s"${baseDir}/${bundleName}.json.zip"
  }
}