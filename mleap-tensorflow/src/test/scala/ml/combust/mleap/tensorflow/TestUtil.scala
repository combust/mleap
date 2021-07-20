package ml.combust.mleap.tensorflow

import java.io.File
import java.nio.file.{Files, Path}

import org.tensorflow
import org.tensorflow.op.Ops
import org.tensorflow.types.TFloat32

/**
  * Created by hollinwilkins on 1/13/17.
  * Updated to TF2.0 by AustinZh on 6/3/2021
  */

object TestUtil {
  def createAddGraph(): tensorflow.Graph = {
    val graph = new tensorflow.Graph
    val tf = Ops.create(graph)
    val inputA = tf.withName("InputA").placeholder(classOf[TFloat32])
    val inputB = tf.withName("InputB").placeholder(classOf[TFloat32])
    tf.withName("MyResult").math.add(inputA, inputB)
    graph
  }

  val baseDir = {
    val temp: Path = Files.createTempDirectory("mleap-tensorflow")
    temp.toFile.deleteOnExit()
    temp.toAbsolutePath
  }

  def delete(file: File): Array[(String, Boolean)] = {
    Option(file.listFiles).map(_.flatMap(f => delete(f))).getOrElse(Array()) :+ (file.getPath -> file.delete)
  }

  def createMultiplyGraph(): tensorflow.Graph = {
    val graph = new tensorflow.Graph
    val tf = Ops.create(graph)
    val inputA = tf.withName("InputA").placeholder(classOf[TFloat32])
    val inputB = tf.withName("InputB").placeholder(classOf[TFloat32])
    tf.withName("MyResult").math.mul(inputA, inputB)
    graph
  }
}
