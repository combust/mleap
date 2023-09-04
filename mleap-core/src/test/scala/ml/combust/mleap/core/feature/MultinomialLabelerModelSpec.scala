package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.funspec.AnyFunSpec

class MultinomialLabelerModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("multinomal labeler model") {
    val model = MultinomialLabelerModel(9.0, ReverseStringIndexerModel(Seq("hello1", "world2", "!3")))


    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double())))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("probabilities", ListType(BasicType.Double)),
        StructField("labels", ListType(BasicType.String))))
    }
  }

}
