package org.apache.spark.ml.bundle

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Node, NodeShape}
import ml.combust.bundle.op.OpNode
import org.apache.spark.ml.Transformer

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 7/4/17.
  */
abstract class SimpleSparkOp[N <: Transformer](implicit ct: ClassTag[N]) extends OpNode[SparkBundleContext, N, N] {
  override val klazz: Class[N] = ct.runtimeClass.asInstanceOf[Class[N]]

  def sparkInputs(obj: N): Seq[ParamSpec]
  def sparkOutputs(obj: N): Seq[ParamSpec]

  override def name(node: N): String = node.uid
  override def model(node: N): N = node

  def sparkLoad(uid: String, shape: NodeShape, model: N): N

  override def load(node: Node, model: N)
                   (implicit context: BundleContext[SparkBundleContext]): N = {
    val n = sparkLoad(node.name, node.shape, model)
    SparkShapeLoader(node.shape, n, sparkInputs(n), sparkOutputs(n)).loadShape()
    n
  }

  override def shape(node: N)
                    (implicit context: BundleContext[SparkBundleContext]): NodeShape = {
    val dataset = context.context.dataset.getOrElse {
      throw new IllegalArgumentException(
        """
          |Must provide a transformed data frame to MLeap for serializing a pipeline.
          |The transformed data frame is used to extract data types and other metadata
          |required for execution.
          |
          |Example usage:
          |```
          |val sparkTransformer: org.apache.spark.ml.Transformer
          |val transformedDataset = sparkTransformer.transform(trainingDataset)
          |
          |implicit val sbc = SparkBundleContext().withDataset(transformedDataset)
          |
          |Using(BundleFile(file)) { bf =>
          |  sparkTransformer.writeBundle.format(SerializationFormat.Json).save(bf).get
          |}
          |```
        """.stripMargin)
    }
    SparkShapeSaver(dataset,
      node,
      sparkInputs(node),
      sparkOutputs(node)).asNodeShape
  }
}
