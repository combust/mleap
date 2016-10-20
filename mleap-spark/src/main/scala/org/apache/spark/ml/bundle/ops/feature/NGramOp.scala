package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl.{Bundle, Model, Node, Shape}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.NGram

/**
  * Created by mikhail on 10/16/16.
  */
object NGramOp extends OpNode[NGram, NGram] {
  override val Model: OpModel[NGram] = new OpModel[NGram] {
    override def opName: String = Bundle.BuiltinOps.feature.ngram

    override def store(context: BundleContext, model: Model, obj: NGram): Model = { model }

    override def load(context: BundleContext, model: Model): NGram = new NGram(uid = "")

  }

  override def name(node: NGram): String = node.uid

  override def model(node: NGram): NGram = node

  override def load(context: BundleContext, node: Node, model: NGram): NGram = {
    new NGram(uid = node.name)
  }

  override def shape(node: NGram): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
