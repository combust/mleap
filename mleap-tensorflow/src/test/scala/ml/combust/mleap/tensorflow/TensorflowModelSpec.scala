package ml.combust.mleap.tensorflow

import java.nio.file.{FileSystems, Files}

import org.tensorflow
import ml.combust.mleap.runtime.types.FloatType
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/12/17.
  */
class TensorflowModelSpec extends FunSpec {
  describe("with a scaling tensorflow model") {
    it("scales the vector using the model and returns the result") {
      val graph = new tensorflow.Graph
      val inputA = graph.opBuilder("Placeholder", "InputA").
        setAttr("dtype", tensorflow.DataType.FLOAT).
        build()
      val inputB = graph.opBuilder("Placeholder", "InputB").
        setAttr("dtype", tensorflow.DataType.FLOAT).
        build()
      graph.opBuilder("Add", "MyResult").
        setAttr("T", tensorflow.DataType.FLOAT).
        addInput(inputA.output(0)).
        addInput(inputB.output(0)).
        build()
      val model = TensorflowModel(graph,
        inputs = Seq(("InputA", FloatType(false)), ("InputB", FloatType(false))),
        outputs = Seq(("MyResult", FloatType(false))))

      assert(model(23.4f, 45.6f).head == 23.4f + 45.6f)
      assert(model(42.3f, 99.9f).head == 42.3f + 99.9f)
      assert(model(65.8f, 34.6f).head == 65.8f + 34.6f)

      model.close()
    }
  }
}
