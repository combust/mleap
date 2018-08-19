package ml.combust.mleap.databricks.runtime.testkit

import java.io.File

import org.tensorflow

/**
  * Created by hollinwilkins on 1/13/17.
  */
object TensorFlowTestUtil {
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

  val baseDir = new File("/tmp/mleap-tensorflow")
  TensorFlowTestUtil.delete(baseDir)
  baseDir.mkdirs()

  def delete(file: File): Array[(String, Boolean)] = {
    Option(file.listFiles).map(_.flatMap(f => delete(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
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
