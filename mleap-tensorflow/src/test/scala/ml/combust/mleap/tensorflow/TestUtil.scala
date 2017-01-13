package ml.combust.mleap.tensorflow

import org.tensorflow

/**
  * Created by hollinwilkins on 1/13/17.
  */
object TestUtil {
  def createAddGraph(): tensorflow.Graph = {
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
    graph
  }

  def createMultiplyGraph(): tensorflow.Graph = {
    val graph = new tensorflow.Graph
    val inputA = graph.opBuilder("Placeholder", "InputA").
      setAttr("dtype", tensorflow.DataType.FLOAT).
      build()
    val inputB = graph.opBuilder("Placeholder", "InputB").
      setAttr("dtype", tensorflow.DataType.FLOAT).
      build()
    graph.opBuilder("Mul", "MyResult").
      setAttr("T", tensorflow.DataType.FLOAT).
      addInput(inputA.output(0)).
      addInput(inputB.output(0)).
      build()
    graph
  }
}
