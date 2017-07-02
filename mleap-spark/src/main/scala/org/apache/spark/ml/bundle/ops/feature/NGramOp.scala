package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.NGram

/**
  * Created by mikhail on 10/16/16.
  */
class NGramOp extends OpNode[SparkBundleContext, NGram, NGram] {
  override val Model: OpModel[SparkBundleContext, NGram] = new OpModel[SparkBundleContext, NGram] {
    override val klazz: Class[NGram] = classOf[NGram]

    override def opName: String = Bundle.BuiltinOps.feature.ngram

    override def store(model: Model, obj: NGram)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("n", Value.long(obj.getN))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): NGram = {
      new NGram(uid = "").setN(model.value("n").getLong.toInt)
    }

  }

  override val klazz: Class[NGram] = classOf[NGram]

  override def name(node: NGram): String = node.uid

  override def model(node: NGram): NGram = node

  override def load(node: Node, model: NGram)
                   (implicit context: BundleContext[SparkBundleContext]): NGram = {
    new NGram(uid = node.name).
      setN(model.getN).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: NGram): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
