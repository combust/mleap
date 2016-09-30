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

    override def store(context: BundleContext, model: Model, obj: StandardScalerModel): Model = {
      val mean = if(obj.getWithMean) Some(obj.mean) else None
      val std = if(obj.getWithStd) Some(obj.std) else None

      model.withAttr(mean.map(m => Attribute("mean", Value.doubleVector(m.toArray)))).
        withAttr(std.map(s => Attribute("std", Value.doubleVector(s.toArray))))
    }

    override def load(context: BundleContext, model: Model): StandardScalerModel = {
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      new StandardScalerModel(uid = "", std = std, mean = mean)
    }
  }

  override def name(node: StandardScalerModel): String = node.uid

  override def model(node: StandardScalerModel): StandardScalerModel = node

  override def load(context: BundleContext, node: Node, model: StandardScalerModel): StandardScalerModel = {
    new StandardScalerModel(uid = node.name, std = model.std, mean = model.mean)
  }

  override def shape(node: StandardScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
