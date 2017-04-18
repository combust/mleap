package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Normalizer

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerOp extends OpNode[SparkBundleContext, Normalizer, Normalizer] {
  override val Model: OpModel[SparkBundleContext, Normalizer] = new OpModel[SparkBundleContext, Normalizer] {
    override val klazz: Class[Normalizer] = classOf[Normalizer]

    override def opName: String = Bundle.BuiltinOps.feature.normalizer

    override def store(model: Model, obj: Normalizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("p_norm", Value.double(obj.getP))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Normalizer = {
      new Normalizer(uid = "").setP(model.value("p_norm").getDouble)
    }
  }

  override val klazz: Class[Normalizer] = classOf[Normalizer]

  override def name(node: Normalizer): String = node.uid

  override def model(node: Normalizer): Normalizer = node

  override def load(node: Node, model: Normalizer)
                   (implicit context: BundleContext[SparkBundleContext]): Normalizer = {
    new Normalizer(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name).
      setP(model.getP)
  }

  override def shape(node: Normalizer)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
