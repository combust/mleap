package ml.combust.bundle.serializer

import java.io.File
import ml.combust.bundle.{BundleFile, BundleRegistry, TestUtil}
import ml.combust.bundle.test.TestContext
import ml.combust.bundle.test.ops._
import org.scalatest.funspec.AnyFunSpec
import ml.combust.bundle.test.TestSupport._
import org.scalatest.matchers.should.Matchers

import scala.util.Using
import scala.util.{Failure, Random}

/**
  * Created by hollinwilkins on 1/16/17.
  */
case class UnknownTransformer() extends Transformer {
  override val uid: String = "haha"
}

class ErrorHandlingSpec extends AnyFunSpec with Matchers {
  implicit val testContext = TestContext(BundleRegistry("test-registry"))
  val randomCoefficients = (0 to 100000).map(v => Random.nextDouble())
  val lr = LinearRegression(uid = "linear_regression_example",
    input = "input_field",
    output = "output_field",
    model = LinearModel(coefficients = randomCoefficients,
      intercept = 44.5))
  val si = StringIndexer(uid = "string_indexer_example",
    input = "input_string",
    output = "output_index",
    model = StringIndexerModel(strings = Seq("hey", "there", "man")))
  val pipeline = Pipeline(uid = "my_pipeline", PipelineModel(Seq(si, lr)))

  describe("with unknown op") {
    it("returns a failure") {
      val result = Using(BundleFile(TestUtil.baseDir.resolve("bad-model.zip"))) { bf =>
        UnknownTransformer().writeBundle.save(bf)
      }.flatten

      result shouldBe a[Failure[_]]
      val Failure(error) = result
      error shouldBe a[NoSuchElementException]
      error.getMessage should (startWith("key not found") and
        include (s"${getClass.getPackageName}.UnknownTransformer"))
    }
  }
}
