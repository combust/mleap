package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.feature.StandardScaler
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
object StandardScalerOp extends OpNode[StandardScaler, StandardScalerModel] {
  override val Model: OpModel[StandardScalerModel] = new OpModel[StandardScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext, model: Model, obj: StandardScalerModel): Model = {
      model.withAttr(obj.mean.map(m => Attribute("mean", Value.doubleVector(m.toArray)))).
        withAttr(obj.std.map(s => Attribute("std", Value.doubleVector(s.toArray))))
    }

    override def load(context: BundleContext, model: Model): StandardScalerModel = {
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense)
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense)
      StandardScalerModel(mean = mean, std = std)
    }
  }

  override def name(node: StandardScaler): String = node.uid

  override def model(node: StandardScaler): StandardScalerModel = node.model

  override def load(context: BundleContext, node: Node, model: StandardScalerModel): StandardScaler = {
    StandardScaler(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StandardScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
