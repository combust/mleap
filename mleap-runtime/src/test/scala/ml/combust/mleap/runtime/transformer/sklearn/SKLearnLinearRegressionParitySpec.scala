package ml.combust.mleap.runtime.transformer.sklearn

import java.io.File
import java.nio.file.{Files, Path, Paths}

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import org.scalactic.TolerantNumerics
import org.scalatest.{BeforeAndAfter, FunSpec}
import resource.managed

import scala.collection.mutable.ListBuffer
import scala.reflect.io.Directory
import scala.sys.process._


class SKLearnLinearModelParitySpec extends FunSpec with BeforeAndAfter {

  val LINEAR_REGRESSION_SCRIPT_PATH = "sklearn_scripts/linear_regression.py"
  val TOLERANCE = 1e-7

  implicit val DoubleEq = TolerantNumerics.tolerantDoubleEquality(TOLERANCE)

  val spark = org.apache.spark.sql.SparkSession.builder.master("local").getOrCreate
  var tempDir: Path = _

  def runPythonTransformer(scriptPath: String, bundlePath: String, csvPath: String): Unit = {
    val resource = getClass.getClassLoader.getResource(scriptPath)
    val absPath = Paths.get(resource.toURI).toFile.getAbsolutePath
    Seq(
      "python3.6", absPath,
      "--bundle-path", bundlePath,
      "--csv-path", csvPath
    ).!
  }

  before {
    tempDir = Files.createTempDirectory(null)
  }

  after {
    val directory = new Directory(tempDir.toFile)
    directory.deleteRecursively()
  }

  describe("sklearn linear regression") {

    it("has the same output as mleap runtime") {
      val bundlePath = tempDir.resolve("logistic_regression_bundle.zip")
      val csvPath = tempDir.resolve("features.csv")
      runPythonTransformer(
        LINEAR_REGRESSION_SCRIPT_PATH,
        tempDir.toString,
        csvPath.toString
      )

      var df = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .option("mode", "DROPMALFORMED")
        .load(csvPath.toString)

      val expectedPredictions = df.select("prediction").collect
      val features = df.drop("label", "prediction")

      val mleapSchema = types.StructType(features.schema.fields.map(f => types.StructField(f.name, types.ScalarType.Double.setNullable(f.nullable)))).get
      val data = features.collect().map {
        r => Row(r.toSeq: _*)
      }
      val leapFrame = DefaultLeapFrame(mleapSchema, data)

      val bundleFile = new File(bundlePath.toString)
      val bundleAbsPath = bundleFile.getAbsolutePath
      val pipeline = managed(BundleFile(s"jar:file:$bundleAbsPath")).acquireAndGet(bundleFile => bundleFile.loadMleapBundle().get).root

      val transformed = pipeline.transform(leapFrame).get

      var results = new ListBuffer[Double]()
      transformed.select("prediction").get.dataset.foreach(
        row => {
          results += row.get(0).asInstanceOf[Double]
        }
      )

      for (i <- 0 until results.length) {
        val expected = expectedPredictions(i).get(0).asInstanceOf[Double].doubleValue
        val actual = results(i).doubleValue
        assert(expected === actual)
      }
    }

  }

}
