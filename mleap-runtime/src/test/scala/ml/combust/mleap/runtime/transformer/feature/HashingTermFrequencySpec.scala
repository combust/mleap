package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.core.types._

class HashingTermFrequencySpec extends org.scalatest.funspec.AnyFunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = HashingTermFrequency(shape = NodeShape().
        withStandardInput("input").
        withStandardOutput("output"), model = HashingTermFrequencyModel())
      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType.Double(262144))))
    }
  }
}