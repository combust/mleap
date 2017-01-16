package ml.combust.mleap.tensorflow

import ml.combust.bundle.dsl.Shape
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.runtime.types.{FloatType, StructField, StructType}
import org.scalatest.FunSpec
import resource._

/**
  * Created by hollinwilkins on 1/13/17.
  */
class TensorflowTransformerSpec extends FunSpec {
  describe("with a scaling tensorflow model") {
    it("scales the vector using the model and returns the result") {
      val bytes = (for(graph <- managed(TestUtil.createAddGraph())) yield {
        graph.toGraphDef
      }).opt.get

      val model = TensorflowModel(bytes,
        inputs = Seq(("InputA", FloatType(false)), ("InputB", FloatType(false))),
        outputs = Seq(("MyResult", FloatType(false))))
      val shape = Shape().withInput("input_a", "InputA").
        withInput("input_b", "InputB").
        withOutput("my_result", "MyResult")
      val transformer = TensorflowTransformer(inputs = shape.inputs,
        outputs = shape.outputs ,
        rawOutputCol = Some("raw_result"),
        model = model)
      val schema = StructType(StructField("input_a", FloatType()), StructField("input_b", FloatType())).get
      val dataset = LocalDataset(Seq(Row(5.6f, 7.9f),
        Row(3.4f, 6.7f),
        Row(1.2f, 9.7f)))
      val frame = LeapFrame(schema, dataset)

      val data = transformer.transform(frame).get.dataset
      assert(data(0)(3) == 5.6f + 7.9f)
      assert(data(1)(3) == 3.4f + 6.7f)
      assert(data(2)(3) == 1.2f + 9.7f)

      transformer.close()
    }
  }
}
