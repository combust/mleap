package ml.combust.mleap.tensorflow

import ml.combust.mleap.runtime.types.{FloatType, TensorType}
import ml.combust.mleap.tensor.DenseTensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/12/17.
  */
class TensorflowModelSpec extends FunSpec {
  describe("with an adding tensorflow model") {
    it("adds two floats together") {
      val model = TensorflowModel(TestUtil.createAddGraph(),
        inputs = Seq(("InputA", FloatType(false)), ("InputB", FloatType(false))),
        outputs = Seq(("MyResult", FloatType(false))))

      assert(model(23.4f, 45.6f).head == 23.4f + 45.6f)
      assert(model(42.3f, 99.9f).head == 42.3f + 99.9f)
      assert(model(65.8f, 34.6f).head == 65.8f + 34.6f)

      model.close()
    }
  }

  describe("with a multiple tensorflow model") {
    describe("with a float and a float vector") {
      it("scales the float vector") {
        val model = TensorflowModel(TestUtil.createMultiplyGraph(),
          inputs = Seq(("InputA", FloatType(false)), ("InputB", TensorType(base = FloatType(false)))),
          outputs = Seq(("MyResult", TensorType(base = FloatType(false)))))
        val tensor1 = DenseTensor(Array(1.0f, 2.0f, 3.0f), Seq(3))
        val scale1 = 2.0f

        assert(model(scale1, tensor1).head.asInstanceOf[DenseTensor[Float]].values sameElements Array(2.0f, 4.0f, 6.0f))
      }
    }
  }
}
