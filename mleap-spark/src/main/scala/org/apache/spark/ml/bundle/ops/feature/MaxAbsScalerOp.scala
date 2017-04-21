package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MaxAbsScalerOp extends OpNode[SparkBundleContext, MaxAbsScalerModel, MaxAbsScalerModel]{
  override val Model: OpModel[SparkBundleContext, MaxAbsScalerModel] = new OpModel[SparkBundleContext, MaxAbsScalerModel] {
    override val klazz: Class[MaxAbsScalerModel] = classOf[MaxAbsScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(model: Model, obj: MaxAbsScalerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("maxAbs", Value.vector(obj.maxAbs.toArray))
  }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MaxAbsScalerModel = {
      new MaxAbsScalerModel(uid = "",
        maxAbs = Vectors.dense(model.value("maxAbs").getTensor[Double].toArray))
    }

  }

  override val klazz: Class[MaxAbsScalerModel] = classOf[MaxAbsScalerModel]

  override def name(node: MaxAbsScalerModel): String = node.uid

  override def model(node: MaxAbsScalerModel): MaxAbsScalerModel = node


  override def load(node: Node, model: MaxAbsScalerModel)
                   (implicit context: BundleContext[SparkBundleContext]): MaxAbsScalerModel = {
    new MaxAbsScalerModel(uid = node.name, maxAbs = model.maxAbs)
  }

  override def shape(node: MaxAbsScalerModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
