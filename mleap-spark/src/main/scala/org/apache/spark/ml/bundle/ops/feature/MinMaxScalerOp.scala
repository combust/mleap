package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MinMaxScalerOp extends OpNode[SparkBundleContext, MinMaxScalerModel, MinMaxScalerModel] {
  override val Model: OpModel[SparkBundleContext, MinMaxScalerModel] = new OpModel[SparkBundleContext, MinMaxScalerModel] {
    override val klazz: Class[MinMaxScalerModel] = classOf[MinMaxScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(model: Model, obj: MinMaxScalerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("min", Value.doubleVector(obj.originalMin.toArray)).
        withAttr("max", Value.doubleVector(obj.originalMax.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MinMaxScalerModel = {
      new MinMaxScalerModel(uid = "",
        originalMin = Vectors.dense(model.value("min").getDoubleVector.toArray),
        originalMax = Vectors.dense(model.value("max").getDoubleVector.toArray))
    }

  }

  override val klazz: Class[MinMaxScalerModel] = classOf[MinMaxScalerModel]

  override def name(node: MinMaxScalerModel): String = node.uid

  override def model(node: MinMaxScalerModel): MinMaxScalerModel = node

  override def load(node: Node, model: MinMaxScalerModel)
                   (implicit context: BundleContext[SparkBundleContext]): MinMaxScalerModel = {
    new MinMaxScalerModel(uid = node.name, originalMin = model.originalMin, originalMax = model.originalMax)
  }

  override def shape(node: MinMaxScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
