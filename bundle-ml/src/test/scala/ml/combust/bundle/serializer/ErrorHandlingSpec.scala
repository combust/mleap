package ml.combust.bundle.serializer

import java.io.File

import ml.combust.bundle.{BundleFile, BundleRegistry, TestUtil}
import ml.combust.bundle.test.TestContext
import ml.combust.bundle.test.ops._
import org.scalatest.FunSpec
import ml.combust.bundle.test.TestSupport._
import resource._

import scala.util.{Failure, Random}

/**
  * Created by hollinwilkins on 1/16/17.
  */
case class UnknownTransformer() extends Transformer {
  override val uid: String = "haha"
}

class ErrorHandlingSpec extends FunSpec {
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
      val result = (for(bf <- managed(BundleFile(new File(TestUtil.baseDir, "bad-model.zip")))) yield {
        UnknownTransformer().writeBundle.save(bf)
      }).tried.flatMap(identity)

      assert(result.isFailure)
      result match {
        case Failure(error) =>
          assert(error.isInstanceOf[NoSuchElementException])
          assert(error.getMessage == "key not found: ml.combust.bundle.serializer.UnknownTransformer")
        case _ =>
      }
    }
  }
}
