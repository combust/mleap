package ml.combust.mleap.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Node, NodeShape}
import ml.combust.bundle.op.OpNode
import ml.combust.mleap.core.types
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.reflect.ClassTag

/**
  * Created by hollinwilkins on 7/3/17.
  */
abstract class MleapOp[N <: Transformer, M <: AnyRef](implicit ct: ClassTag[N]) extends OpNode[MleapContext, N, M] {
  override val klazz: Class[N] = ct.runtimeClass.asInstanceOf[Class[N]]

  override def name(node: N): String = node.uid

  override def load(node: Node, model: M)
                   (implicit context: BundleContext[MleapContext]): N = {
    klazz.getConstructor(classOf[String], classOf[types.NodeShape], Model.klazz).
      newInstance(node.name, node.shape.asBundle: types.NodeShape, model)
  }

  override def shape(node: N)
                    (implicit context: BundleContext[MleapContext]): NodeShape = NodeShape.fromBundle(node.shape)
}
