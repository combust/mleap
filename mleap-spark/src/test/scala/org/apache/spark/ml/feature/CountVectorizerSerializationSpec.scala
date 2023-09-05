package org.apache.spark.ml.feature

import java.io.File
import java.nio.file.{Files, Path}
import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Using}

class CountVectorizerSerializationSpec extends AnyFunSpec with Matchers {
  private val spark = SparkSession.builder()
    .config("spark.ui.enabled", "false")
    .appName("CountVectorizerSerializationSpec")
    .master("local[2]").getOrCreate()
  val df = spark.createDataFrame(
    rows = java.util.Arrays.asList(
      Row(Array("a", "b", "a", "a", "b", null)),
      Row(Array("b", null, null))
    ),
    schema = StructType(Seq(
      StructField("feature", ArrayType(StringType, true), false)
    ))
  )

  it("raises an informative error given null vocabulary") {
    val sparkTransformer: Transformer = new Pipeline().setStages(Array(
      new CountVectorizer()
        .setInputCol("feature")
        .setOutputCol("feature_counts")
    )).fit(df)
    val transformedDF = sparkTransformer.transform(df)

    implicit val context = SparkBundleContext().withDataset(transformedDF)
    val tempDirPath = {
      val temp: Path = Files.createTempDirectory("CountVectorizerSerializationSpec")
      temp.toFile.deleteOnExit()
      temp.toAbsolutePath
    }
    val file = new File(s"${tempDirPath}/${getClass.getName}.zip")

    val attempt = Using(BundleFile(file)) { bf =>
        sparkTransformer.writeBundle.format(SerializationFormat.Json).save(bf)
      }.flatten
    attempt shouldBe a [Failure[_]]
    val Failure(caughtException) = attempt

    caughtException.getMessage shouldBe "MLeap cannot serialize CountVectorizerModel vocabularies containing `null`"
  }

}
