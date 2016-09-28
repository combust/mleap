package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ElementwiseProductModel
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{StructField, StructType, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 9/28/16.
  */
class ElementWiseProductSpec extends FunSpec {
  val schema = StructType(Seq(StructField("test_vec", TensorType.doubleVector()))).get
  val dataset = LocalDataset(Array(Row(Vectors.dense(Array(0.0, 20.0, 20.0)))))
  val frame = LeapFrame(schema, dataset)

  val ewp = ElementwiseProduct(inputCol = "test_vec",
    outputCol = "test_norm",
    model = ElementwiseProductModel(Vectors.dense(Array(0.5, 1.0, 0.5))))

  describe("#transform") {
    it("multiplies each input vector by a provided weight vector") {
      val frame2 = ewp.transform(frame).get
      val data = frame2.dataset.toArray(0).getVector(1)

      assert(data.toArray sameElements Array(0.0, 20.0, 10.0))
    }

    describe("with invalid input column") {
      val ewp2 = ewp.copy(inputCol = "bad_input")

      it("returns a Failure") { assert(ewp2.transform(frame).isFailure) }
    }
  }
}
