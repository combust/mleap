package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/21/16.
  */
class StandardScalerOp extends OpNode[SparkBundleContext, StandardScalerModel, StandardScalerModel] {
  override val Model: OpModel[SparkBundleContext, StandardScalerModel] = new OpModel[SparkBundleContext, StandardScalerModel] {
    override val klazz: Class[StandardScalerModel] = classOf[StandardScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: StandardScalerModel): Model = {
      val mean = if(obj.getWithMean) Some(obj.mean.toArray.toSeq) else None
      val std = if(obj.getWithStd) Some(obj.std.toArray.toSeq) else None

      model.withAttr("mean", mean.map(Value.doubleVector)).
        withAttr("std", std.map(Value.doubleVector))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): StandardScalerModel = {
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense).orNull
      new StandardScalerModel(uid = "", std = std, mean = mean)
    }
  }

  override val klazz: Class[StandardScalerModel] = classOf[StandardScalerModel]

  override def name(node: StandardScalerModel): String = node.uid

  override def model(node: StandardScalerModel): StandardScalerModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: StandardScalerModel): StandardScalerModel = {
    new StandardScalerModel(uid = node.name, std = model.std, mean = model.mean)
  }

  override def shape(node: StandardScalerModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
