package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.MinHashLSHModel

/**
  * Created by hollinwilkins on 12/28/16.
  */
class MinHashLSHOp extends OpNode[SparkBundleContext, MinHashLSHModel, MinHashLSHModel] {
  override val Model: OpModel[SparkBundleContext, MinHashLSHModel] = new OpModel[SparkBundleContext, MinHashLSHModel] {
    override val klazz: Class[MinHashLSHModel] = classOf[MinHashLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_hash_lsh

    override def store(model: Model, obj: MinHashLSHModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val (ca, cb) = obj.randCoefficients.unzip

      model.withAttr("random_coefficients_a", Value.longList(ca.map(_.toLong))).
        withAttr("random_coefficients_b", Value.longList(cb.map(_.toLong)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MinHashLSHModel = {
      val ca = model.value("random_coefficients_a").getLongList.map(_.toInt)
      val cb = model.value("random_coefficients_b").getLongList.map(_.toInt)
      val randomCoefficients = ca.zip(cb)
      new MinHashLSHModel(uid = "", randCoefficients = randomCoefficients.toArray)
    }
  }

  override val klazz: Class[MinHashLSHModel] = classOf[MinHashLSHModel]

  override def name(node: MinHashLSHModel): String = node.uid

  override def model(node: MinHashLSHModel): MinHashLSHModel = node

  override def load(node: Node, model: MinHashLSHModel)
                   (implicit context: BundleContext[SparkBundleContext]): MinHashLSHModel = {
    val m = new MinHashLSHModel(uid = node.name, randCoefficients = model.randCoefficients)
    m.set(m.inputCol, node.shape.standardInput.name)
    m.set(m.outputCol, node.shape.standardOutput.name)

    m
  }

  override def shape(node: MinHashLSHModel): NodeShape = NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
