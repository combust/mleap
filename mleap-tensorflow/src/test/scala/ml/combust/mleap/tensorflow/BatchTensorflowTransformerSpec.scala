package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.{NodeShape, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{BatchLeapFrame, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

class BatchTensorflowTransformerSpec extends FunSpec {
  describe("with a scaling tensorflow model") {
    it("scales the vector using the model and returns the result") {
      val model = BatchTensorflowModel(TestUtil.createAddGraph(),
        inputs = Seq(("InputA", TensorType.Float()), ("InputB", TensorType.Float())),
        outputs = Seq(("MyResult", TensorType.Float())))
      val shape = NodeShape().withInput("InputA", "input_a").
        withInput("InputB", "input_b").
        withOutput("MyResult", "my_result")
      val transformer = BatchTensorflowTransformer(uid = "tensorflow_ab",
        shape = shape,
        model = model)
      val schema = StructType(StructField("input_a", TensorType.Float()), StructField("input_b", TensorType.Float())).get
      val dataset = Seq(Row(5.6f, 7.9f),
        Row(3.4f, 6.7f),
        Row(1.2f, 9.7f))
      val frame = BatchLeapFrame(schema, dataset)

      val data = transformer.transform(frame).get.dataset
      assert(data(0)(2) == Tensor.scalar(5.6f + 7.9f))
      assert(data(1)(2) == Tensor.scalar(3.4f + 6.7f))
      assert(data(2)(2) == Tensor.scalar(1.2f + 9.7f))

      transformer.close()
    }
  }
}