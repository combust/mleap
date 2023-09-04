package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class CountVectorizerModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("binarizer with one input") {
    val model = new CountVectorizerModel(Array("1", "2", "3"), true, 2)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(3))))
    }

    it("Produces the correct mleap Tensor"){
      assert(model(Seq("1", "1", "2", "3")).toArray.toSeq == Seq(1, 0, 0))
    }
  }
}