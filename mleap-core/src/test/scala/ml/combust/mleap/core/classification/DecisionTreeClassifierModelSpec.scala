package ml.combust.mleap.core.classification

import ml.combust.mleap.core.types.{ScalarType, StructField, TensorType}
import org.scalatest.funspec.AnyFunSpec

class DecisionTreeClassifierModelSpec extends org.scalatest.funspec.AnyFunSpec {

  describe("decision tree classifier model") {
    val model = DecisionTreeClassifierModel(null, 3, 2)

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("features", TensorType.Double(3))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("raw_prediction", TensorType.Double(2)),
          StructField("probability", TensorType.Double(2)),
          StructField("prediction", ScalarType.Double.nonNullable)
        ))
    }
  }
}
