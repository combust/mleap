package ml.combust.mleap.executor

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.regression.LinearRegressionModel
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.runtime.transformer.regression.LinearRegression
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{FunSpec, Matchers}
import ml.combust.mleap.core.types._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

class ExecuteTransformSpec extends FunSpec with ScalaFutures with Matchers {

  describe("execute transform") {

    val pipeline = Pipeline("pipeline", NodeShape(),
      PipelineModel(Seq(
        VectorAssembler(shape = NodeShape().withInput("input0", "first_double").
          withInput("input1", "second_double").
          withStandardOutput("features"),
        model = VectorAssemblerModel(Seq(ScalarShape(), ScalarShape()))),
        LinearRegression(shape = NodeShape.regression(),
        model = LinearRegressionModel(Vectors.dense(2.0, 2.0), 5.0)))))

    val input = DefaultLeapFrame(StructType(Seq(StructField("first_double", ScalarType.Double),
      StructField("second_double" -> ScalarType.Double))).get,
      Seq(Row(20.0, 10.0)))

    it("transforms successfully a leap frame in strict mode") {
      val request = TransformFrameRequest(Success(input), TransformOptions(Some(Seq("features", "prediction")), SelectMode.Strict))
      val result = ExecuteTransform(transformer = pipeline , request = request)

      whenReady(result) {
         frame => {
           val data = frame.collect().head

           assert(frame.schema.fields.length == 2)
           assert(frame.schema.indexOf("features").get == 0)
           assert(data.getTensor(0) == Tensor.denseVector(Array(20.0, 10.0)))
           assert(data.getDouble(1) == 65.0)
         }
      }
    }

    it("throws exception when transforming and selecting a missing field in strict mode") {
      val request = TransformFrameRequest(Success(input), TransformOptions(Some(Seq("features", "prediction", "does-not-exist")), SelectMode.Strict))
      val result = ExecuteTransform(transformer = pipeline , request = request)

      whenReady(result.failed) {
        ex => ex shouldBe a [IllegalArgumentException]
      }
    }

    it("transforms successfully a leap frame in relaxed mode, ignoring unknown fields") {
      val request = TransformFrameRequest(Success(input), TransformOptions(Some(Seq("features", "prediction", "does-not-exist")), SelectMode.Relaxed))
      val result = ExecuteTransform(transformer = pipeline , request = request)

      whenReady(result) {
        frame => {
          val data = frame.collect().head

          assert(frame.schema.fields.length == 2)
          assert(frame.schema.indexOf("features").get == 0)
          assert(data.getTensor(0) == Tensor.denseVector(Array(20.0, 10.0)))
          assert(data.getDouble(1) == 65.0)
        }
      }
    }

    it("throws exception when transforming throws exception") {
      val invalidPipeline = Pipeline("pipeline", NodeShape(),
        PipelineModel(Seq(
          VectorAssembler(shape = NodeShape().withInput("input0", "first_double").
            withInput("input1", "second_double").
            withStandardOutput("features"),
            model = VectorAssemblerModel(Seq(ScalarShape(), ScalarShape()))),
          LinearRegression(shape = NodeShape.regression(),
            // missing coefficient for LR
            model = LinearRegressionModel(Vectors.dense(2.0), 5.0)))))
      val request = TransformFrameRequest(Success(input), TransformOptions(Some(Seq("features", "prediction")), SelectMode.Strict))
      val result = ExecuteTransform(transformer = invalidPipeline , request = request)

      whenReady(result.failed) {
        ex => ex shouldBe a [IllegalArgumentException]
      }
    }
  }
}
