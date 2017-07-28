package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.WordToVectorModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class WordToVectorSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = WordToVector(shape = NodeShape().withStandardInput("input").
              withStandardOutput("output"),
              model =  WordToVectorModel(Map("test" -> 1), Array(12)))

      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType.Double())))
    }
  }
}