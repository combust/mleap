package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class HashingTermFrequencySpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = HashingTermFrequency(shape = NodeShape().
              withStandardInput("input").
              withStandardOutput("output"), model = HashingTermFrequencyModel())
      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType.Double())))
    }
  }
}