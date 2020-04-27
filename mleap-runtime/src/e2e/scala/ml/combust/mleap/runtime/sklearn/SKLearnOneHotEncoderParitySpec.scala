package ml.combust.mleap.runtime.transformer.sklearn

import java.io.File
import java.nio.file.{Files, Path, Paths}

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import ml.combust.mleap.runtime.transformer.feature.OneHotEncoder
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.mleap.tensor.SparseTensor
import org.scalatest.{BeforeAndAfter, FunSpec}
import resource.managed

import scala.reflect.io.Directory
import scala.sys.process._


class SKLearnOneHotEncoderParitySpec extends FunSpec with BeforeAndAfter {

  val SCRIPT_PATH = "sklearn_scripts/one_hot_encoder.py"

  val spark = org.apache.spark.sql.SparkSession.builder.master("local").getOrCreate
  var tempDir: Path = _

  def runPythonTransformer(scriptPath: String, bundleDir: String, originalCSVPath: String, transformedCSVPath: String): Unit = {
    val resource = getClass.getClassLoader.getResource(scriptPath)
    val absPath = Paths.get(resource.toURI).toFile.getAbsolutePath
    Seq(
      "python3.6", absPath,
      "--bundle-dir", bundleDir,
      "--original-csv-path", originalCSVPath,
      "--transformed-csv-path", transformedCSVPath
    ).!
  }

  def getOneHotEncoders(pipeline: Pipeline): Seq[Transformer] = {
    var oneHotEncoders = Seq(): Seq[Transformer]
    for (transformer <- pipeline.model.asInstanceOf[PipelineModel].transformers) {
      if (transformer.isInstanceOf[Pipeline]) {
        oneHotEncoders = oneHotEncoders ++ getOneHotEncoders(transformer.asInstanceOf[Pipeline])
      } else if (transformer.isInstanceOf[OneHotEncoder]) {
        oneHotEncoders = oneHotEncoders :+ transformer
      }
    }
    oneHotEncoders
  }

  before {
    tempDir = Files.createTempDirectory(null)
  }

  after {
    val directory = new Directory(tempDir.toFile)
    directory.deleteRecursively()
  }

  describe("sklearn one hot encoder") {

    it("has the same output as the mleap runtime") {

      // Run python transformer
      val bundlePath = tempDir.resolve("one_hot_encoder_bundle.zip")
      val originalCSVPath = tempDir.resolve("original.csv")
      val transformedCSVPath = tempDir.resolve("transformed.csv")
      runPythonTransformer(
        scriptPath = SCRIPT_PATH,
        bundleDir = tempDir.toString,
        originalCSVPath = originalCSVPath.toString,
        transformedCSVPath = transformedCSVPath.toString
      )

      // Load input data
      val inputDF = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .option("mode", "DROPMALFORMED")
        .load(originalCSVPath.toString)

      // Convert to leap frame
      val inputLeapFrame = DefaultLeapFrame(
        types.StructType(inputDF.schema.fields.map(f => types.StructField(f.name, types.ScalarType.String.setNullable(f.nullable)))).get,
        inputDF.collect().map {
          r => Row(r.toSeq: _*)
        }
      )

      // Run scala transformer
      val bundleFile = new File(bundlePath.toString)
      val bundleAbsPath = bundleFile.getAbsolutePath
      val pipeline = managed(BundleFile(s"jar:file:$bundleAbsPath")).acquireAndGet(bundleFile => bundleFile.loadMleapBundle().get).root
      val actualLeapFrame = pipeline.transform(inputLeapFrame).get

      // Get scala results
      val outputColumnNames = getOneHotEncoders(pipeline.asInstanceOf[Pipeline]).map(_.outputSchema.fields(0).name)
      val output = actualLeapFrame.select(outputColumnNames: _*).get.collect()
      val actualRows = output.map(row =>
        row.map(_.asInstanceOf[SparseTensor[Double]].toArray).flatten.toSeq
      )

      // Get python results
      val expectedRows = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .option("mode", "DROPMALFORMED")
        .load(transformedCSVPath.toString)
        .collect()

      // Check for python/scala parity
      for ((expectedRow, actualRow) <- expectedRows zip actualRows) {
        assert(expectedRow.length == actualRow.length)
        for (i <- 0 until expectedRow.length) {
          assert(expectedRow(i) == actualRow(i))
        }
      }

    }
  }
}