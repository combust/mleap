package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Tokenizer

/**
  * Created by hollinwilkins on 8/21/16.
  */
class TokenizerOp extends OpNode[SparkBundleContext, Tokenizer, Tokenizer] {
  override val Model: OpModel[SparkBundleContext, Tokenizer] = new OpModel[SparkBundleContext, Tokenizer] {
    override val klazz: Class[Tokenizer] = classOf[Tokenizer]

    override def opName: String = Bundle.BuiltinOps.feature.tokenizer

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: Tokenizer): Model = { model }

    override def load(context: BundleContext[SparkBundleContext], model: Model): Tokenizer = new Tokenizer(uid = "")
  }

  override val klazz: Class[Tokenizer] = classOf[Tokenizer]

  override def name(node: Tokenizer): String = node.uid

  override def model(node: Tokenizer): Tokenizer = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: Tokenizer): Tokenizer = {
    new Tokenizer(uid = node.name)
  }

  override def shape(node: Tokenizer): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
