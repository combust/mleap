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

    override def store(model: Model, obj: StandardScalerModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val mean = if(obj.getWithMean) Some(obj.mean.toArray) else None
      val std = if(obj.getWithStd) Some(obj.std.toArray) else None

      model.withAttr("mean", mean.map(Value.vector)).
        withAttr("std", std.map(Value.vector))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): StandardScalerModel = {
      val std = model.getValue("std").map(_.getTensor[Double].toArray).map(Vectors.dense)
      val mean = model.getValue("mean").map(_.getTensor[Double].toArray).map(Vectors.dense)
      val size = std.map(_.size).orElse(mean.map(_.size)).get

      val m = new StandardScalerModel(uid = "",
        std = std.getOrElse(Vectors.sparse(size, Array(), Array())),
        mean = mean.getOrElse(Vectors.sparse(size, Array(), Array())))
      std.foreach(_ => m.set(m.withStd, true))
      mean.foreach(_ => m.set(m.withMean, true))
      m
    }
  }

  override val klazz: Class[StandardScalerModel] = classOf[StandardScalerModel]

  override def name(node: StandardScalerModel): String = node.uid

  override def model(node: StandardScalerModel): StandardScalerModel = node

  override def load(node: Node, model: StandardScalerModel)
                   (implicit context: BundleContext[SparkBundleContext]): StandardScalerModel = {
    new StandardScalerModel(uid = node.name, std = model.std, mean = model.mean).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: StandardScalerModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
