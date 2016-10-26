package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MaxAbsScalerOp extends OpNode[SparkBundleContext, MaxAbsScalerModel, MaxAbsScalerModel]{
  override val Model: OpModel[SparkBundleContext, MaxAbsScalerModel] = new OpModel[SparkBundleContext, MaxAbsScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: MaxAbsScalerModel): Model = {
      model.withAttr("maxAbs", Value.doubleVector(obj.maxAbs.toArray))
  }

    override def load(context: BundleContext[SparkBundleContext], model: Model): MaxAbsScalerModel = {
      new MaxAbsScalerModel(uid = "",
        maxAbs = Vectors.dense(model.value("maxAbs").getDoubleVector.toArray))
    }

  }

  override def name(node: MaxAbsScalerModel): String = node.uid

  override def model(node: MaxAbsScalerModel): MaxAbsScalerModel = node


  override def load(context: BundleContext[SparkBundleContext], node: Node, model: MaxAbsScalerModel): MaxAbsScalerModel = {
    new MaxAbsScalerModel(uid = node.name, maxAbs = model.maxAbs)
  }

  override def shape(node: MaxAbsScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
