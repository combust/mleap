package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class WordToVectorSpec extends FunSpec {

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = WordToVector(shape = NodeShape().withStandardInput("input", ListType(BasicType.String)).
        withStandardOutput("output", TensorType(BasicType.Double, Seq(4))), model = null)

      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType(BasicType.Double, Seq(4)))))
    }
  }
}