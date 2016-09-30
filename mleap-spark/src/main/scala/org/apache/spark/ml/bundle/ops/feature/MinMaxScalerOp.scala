package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.MinMaxScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MinMaxScalerOp extends OpNode[MinMaxScalerModel, MinMaxScalerModel] {
  override val Model: OpModel[MinMaxScalerModel] = new OpModel[MinMaxScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(context: BundleContext, model: Model, obj: MinMaxScalerModel): Model = {
      model.withAttr(Attribute("min", Value.doubleVector(obj.originalMin.toArray))).
        withAttr(Attribute("max", Value.doubleVector(obj.originalMax.toArray)))
    }

    override def load(context: BundleContext, model: Model): MinMaxScalerModel = {
      new MinMaxScalerModel(uid = "",
        originalMin = Vectors.dense(model.value("min").getDoubleVector.toArray),
        originalMax = Vectors.dense(model.value("max").getDoubleVector.toArray))
    }

  }

  override def name(node: MinMaxScalerModel): String = node.uid

  override def model(node: MinMaxScalerModel): MinMaxScalerModel = node

  override def load(context: BundleContext, node: Node, model: MinMaxScalerModel): MinMaxScalerModel = {
    new MinMaxScalerModel(uid = node.name, originalMin = model.originalMin, originalMax = model.originalMax)
  }

  override def shape(node: MinMaxScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
