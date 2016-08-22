package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.feature.StandardScaler
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.bundle.dsl._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object StandardScalerOp extends OpNode[StandardScaler, StandardScalerModel] {
  override val Model: OpModel[StandardScalerModel] = new OpModel[StandardScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext, model: WritableModel, obj: StandardScalerModel): WritableModel = {
      var model2 = model
      model2 = obj.mean match {
        case Some(mean) => model2.withAttr(Attribute("mean", Value.doubleVector(mean.toArray)))
        case None => model2
      }
      model2 = obj.std match {
        case Some(std) => model2.withAttr(Attribute("std", Value.doubleVector(std.toArray)))
        case None => model2
      }
      model2
    }

    override def load(context: BundleContext, model: ReadableModel): StandardScalerModel = {
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense)
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense)
      StandardScalerModel(mean = mean, std = std)
    }
  }

  override def name(node: StandardScaler): String = node.uid

  override def model(node: StandardScaler): StandardScalerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: StandardScalerModel): StandardScaler = {
    StandardScaler(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StandardScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
