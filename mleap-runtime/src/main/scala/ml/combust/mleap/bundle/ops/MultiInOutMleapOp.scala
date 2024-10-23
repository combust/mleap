package ml.combust.mleap.bundle.ops

import ml.bundle.Socket
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Node, NodeShape}
import ml.combust.mleap.core.types
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.reflect.ClassTag

abstract class MultiInOutMleapOp[N <: Transformer, M <: AnyRef](implicit ct: ClassTag[N]) extends MleapOp[N, M] {
  override def load(node: Node, model: M)(implicit context: BundleContext[MleapContext]): N = {
    val ns = node.shape.getInput(NodeShape.standardInputPort) match { // Old version need to translate serialized port names to new expectation (input -> input0)
      case Some(_) ⇒ translateLegacyShape(node.shape)

      // New version
      case None ⇒ node.shape
    }
    klazz.getConstructor(classOf[String], classOf[types.NodeShape], Model.klazz).newInstance(node.name, ns.asBundle: types.NodeShape, model)
  }

  private def translateLegacyShape(ns: NodeShape): NodeShape = {
    val i = ns.getInput(NodeShape.standardInputPort).get
    val o = ns.getOutput(NodeShape.standardOutputPort).get
    NodeShape(inputs = Seq(Socket(i.port + "0", i.name)), outputs = Seq(Socket(o.port + "0", o.name)))
  }
}