package ml.combust.mleap.tensorflow

import ml.combust.mleap.runtime.types.FloatType
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/12/17.
  */
class TensorflowModelSpec extends FunSpec {
  describe("with a scaling tensorflow model") {
    it("scales the vector using the model and returns the result") {
      val model = TensorflowModel(TestUtil.createAddGraph(),
        inputs = Seq(("InputA", FloatType(false)), ("InputB", FloatType(false))),
        outputs = Seq(("MyResult", FloatType(false))))

      assert(model(23.4f, 45.6f).head == 23.4f + 45.6f)
      assert(model(42.3f, 99.9f).head == 42.3f + 99.9f)
      assert(model(65.8f, 34.6f).head == 65.8f + 34.6f)

      model.close()
    }
  }
}
