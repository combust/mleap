package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
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

    override def store(model: Model, obj: Tokenizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = { model }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Tokenizer = new Tokenizer(uid = "")
  }

  override val klazz: Class[Tokenizer] = classOf[Tokenizer]

  override def name(node: Tokenizer): String = node.uid

  override def model(node: Tokenizer): Tokenizer = node

  override def load(node: Node, model: Tokenizer)
                   (implicit context: BundleContext[SparkBundleContext]): Tokenizer = {
    new Tokenizer(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Tokenizer): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
