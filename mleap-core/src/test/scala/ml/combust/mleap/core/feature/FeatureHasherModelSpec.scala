package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types._
import org.scalatest.FunSpec

class FeatureHasherModelSpec  extends FunSpec {

  val schema = Seq(
    StructField("doubleCol", DataType(BasicType.Double, ScalarShape())),
    StructField("boolCol", DataType(BasicType.Boolean, ScalarShape())),
    StructField("intCol", DataType(BasicType.Int, ScalarShape())),
    StructField("stringCol", DataType(BasicType.String, ScalarShape()))
  )

  describe("Hashing Term Frequency Model") {
    val model = FeatureHasherModel(
      categoricalCols = Seq.empty[String],
      inputNames = schema.map(_.name),
      inputTypes = schema.map(_.dataType)
    )

    it("Has the right input schema") {
      assert(model.inputSchema.fields == schema.zipWithIndex.map({ case (sf, idx) ⇒
          sf.copy(name = s"input$idx")
      }))
    }

    it("Has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(262144))))
    }
  }
}
