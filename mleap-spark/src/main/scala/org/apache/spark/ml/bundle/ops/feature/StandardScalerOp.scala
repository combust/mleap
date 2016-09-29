package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
object StandardScalerOp extends OpNode[StandardScalerModel, StandardScalerModel] {
  override val Model: OpModel[StandardScalerModel] = new OpModel[StandardScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext, model: WritableModel, obj: StandardScalerModel): WritableModel = {
      var model2 = model
      model2 = if(obj.getWithMean) {
        model2.withAttr(Attribute("mean", Value.doubleVector(obj.mean.toArray)))
      } else { model2 }
      model2 = if(obj.getWithStd) {
        model.withAttr(Attribute("std", Value.doubleVector(obj.std.toArray)))
      } else { model2 }
      model2
    }

    override def load(context: BundleContext, model: ReadableModel): StandardScalerModel = {
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      new StandardScalerModel(uid = "", std = std, mean = mean)
    }
  }

  override def name(node: StandardScalerModel): String = node.uid

  override def model(node: StandardScalerModel): StandardScalerModel = node

  override def load(context: BundleContext, node: ReadableNode, model: StandardScalerModel): StandardScalerModel = {
    new StandardScalerModel(uid = node.name, std = model.std, mean = model.mean)
  }

  override def shape(node: StandardScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
