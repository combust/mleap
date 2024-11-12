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
    assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))
    SparkShapeSaver(context.context.dataset.get,
      node,
      sparkInputs(node),
      sparkOutputs(node)).asNodeShape
  }
}
