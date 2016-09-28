package org.apache.spark.ml.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MaxAbsScalerOp extends OpNode[MaxAbsScalerModel, MaxAbsScalerModel]{
  override val Model: OpModel[MaxAbsScalerModel] = new OpModel[MaxAbsScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(context: BundleContext, model: WritableModel, obj: MaxAbsScalerModel): WritableModel = {
      model.withAttr(Attribute("maxAbs", Value.doubleVector(obj.maxAbs.toArray)))
  }

    override def load(context: BundleContext, model: ReadableModel): MaxAbsScalerModel = {
      new MaxAbsScalerModel(uid = "",
        maxAbs = Vectors.dense(model.value("maxAbs").getDoubleVector.toArray))
    }

  }

  override def name(node: MaxAbsScalerModel): String = node.uid

  override def model(node: MaxAbsScalerModel): MaxAbsScalerModel = node


  override def load(context: BundleContext, node: ReadableNode, model: MaxAbsScalerModel): MaxAbsScalerModel = {
    new MaxAbsScalerModel(uid = node.name, maxAbs = model.maxAbs)
  }

  override def shape(node: MaxAbsScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
