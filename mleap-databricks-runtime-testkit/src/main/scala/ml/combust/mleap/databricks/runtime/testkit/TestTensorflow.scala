package ml.combust.mleap.databricks.runtime.testkit

import java.io.File
import java.nio.file.Files

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types.{NodeShape, TensorType}
import ml.combust.mleap.tensorflow.{TensorflowModel, TensorflowTransformer}
import org.apache.spark.sql.SparkSession
import ml.combust.mleap.runtime.MleapSupport._

class TestTensorflow(session: SparkSession) extends Runnable {
  override def run(): Unit = {
    val graph = TensorFlowTestUtil.createAddGraph()
    val model = TensorflowModel(graph = Some(graph),
      inputs = Seq(("InputA", TensorType.Float()), ("InputB", TensorType.Float())),
      outputs = Seq(("MyResult", TensorType.Float())),
      modelBytes = graph.toGraphDef.toByteArray
    )
    val shape = NodeShape().withInput("InputA", "input_a").
      withInput("InputB", "input_b").
      withOutput("MyResult", "my_result")
    val transformer = TensorflowTransformer(uid = "tensorflow_ab",
      shape = shape,
      model = model)

    val modelPath = Files.createTempFile("mleap-databricks-runtime-testkit", ".zip")
    Files.delete(modelPath)

    {
      println("Writing model to...", modelPath)
      val bf = BundleFile(new File(modelPath.toString))
      transformer.writeBundle.save(bf).get
      bf.close()
    }

    {
      val bf = BundleFile(new File(modelPath.toString))
      bf.loadMleapBundle()
      bf.close()
    }
  }
}
