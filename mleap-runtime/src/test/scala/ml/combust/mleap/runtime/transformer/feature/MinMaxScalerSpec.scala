package ml.combust.mleap.runtime.transformer.feature

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.feature.MinMaxScalerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.runtime.transformer.Pipeline
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors

import java.io.File
import scala.util.Using

/**
 * Created by mikhail on 9/25/16.
 */
class MinMaxScalerSpec extends org.scalatest.funspec.AnyFunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
  val dataset = Seq(Row(Tensor.denseVector(Array(0.0, 20.0, 20.0))))
  val frame = DefaultLeapFrame(schema, dataset)

  val minMaxScaler = MinMaxScaler(
    shape = NodeShape.feature(inputCol = "test_vec", outputCol = "test_normalized"),
    model = MinMaxScalerModel(Vectors.dense(Array(0.0, 0.0, 0.0)), Vectors.dense(Array(10.0, 20.0, 40.0))))

  describe("#transform") {
    it("scales the input data between min / max value vectors") {
      val frame2 = minMaxScaler.transform(frame).get
      val data = frame2.dataset.toArray
      val norm = data(0).getTensor[Double](1)

      assert(norm(0) == 0.0)
      assert(norm(1) == 1.0)
      assert(norm(2) == 0.5)
    }
    describe("with invalid input column") {
      val minMaxScaler2 = minMaxScaler.copy(shape = NodeShape.feature(inputCol = "bad_feature"))

      it("returns a Failure") {
        assert(minMaxScaler2.transform(frame).isFailure)
      }
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(minMaxScaler.schema.fields ==
        Seq(StructField("test_vec", TensorType.Double(3)),
          StructField("test_normalized", TensorType.Double(3))))
    }
  }

  describe("min max scaler with defaults for min/max still works") {
    it("loads correctly in mleap") {
      val file = new File(getClass.getResource("/min_max_scaler_tf.zip").toURI)
      val pipeline = Using(BundleFile(file)) { bf =>
        bf.loadMleapBundle().get.root
      }.get.asInstanceOf[Pipeline]

      assert(pipeline.model.transformers.size == 2)
    }
  }
}
