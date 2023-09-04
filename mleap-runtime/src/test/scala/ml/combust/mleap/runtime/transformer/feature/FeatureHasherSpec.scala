package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.FeatureHasherModel
import ml.combust.mleap.core.types._

class FeatureHasherSpec extends org.scalatest.funspec.AnyFunSpec {

  val inputSchema = Map(
    "input0" → StructField("doubleCol", DataType(BasicType.Double, ScalarShape())),
    "input1" → StructField("boolCol", DataType(BasicType.Boolean, ScalarShape())),
    "input2" → StructField("intCol", DataType(BasicType.Int, ScalarShape())),
    "input3" → StructField("stringCol", DataType(BasicType.String, ScalarShape()))
  )
  val outputSchema = Map(
    "output" → StructField("features", TensorType.Double(262144))
  )

  val model = FeatureHasherModel(
    categoricalCols = Seq.empty[String],
    inputNames = inputSchema.values.map(_.name).toSeq,
    inputTypes = inputSchema.values.map(_.dataType).toSeq
  )

  describe("input/output schema") {

    val shape = inputSchema.foldLeft(NodeShape()) { (acc, item) ⇒
      acc.withInput(item._1, item._2.name)
    }.withOutput(outputSchema.head._1, outputSchema.head._2.name)

    it("has the correct inputs and outputs") {
      val transformer = FeatureHasher(
        shape = shape,
        model = model)

      assert(transformer.schema.fields == inputSchema.values ++ outputSchema.values)
    }
  }
}