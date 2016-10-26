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

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: Normalizer): Model = {
      model.withAttr("p_norm", Value.double(obj.getP))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): Normalizer = {
      new Normalizer(uid = "").setP(model.value("p_norm").getDouble)
    }
  }

  override val klazz: Class[Normalizer] = classOf[Normalizer]

  override def name(node: Normalizer): String = node.uid

  override def model(node: Normalizer): Normalizer = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: Normalizer): Normalizer = {
    new Normalizer(uid = node.name).copy(model.extractParamMap()).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: Normalizer): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
