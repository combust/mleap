package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CountVectorizerModel
import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class CountVectorizerSpec extends FunSpec {

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = CountVectorizer(shape = NodeShape().
                    withStandardInput("input").
              withStandardOutput("output"),
        model = new CountVectorizerModel(Array("1", "2", "3"), true, 2))
      assert(transformer.schema.fields ==
        Seq(StructField("input", ListType(BasicType.String)),
          StructField("output", TensorType.Double(3))))
    }
  }
}