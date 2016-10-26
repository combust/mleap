package ml.combust.mleap.bundle.ops.feature

import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.feature.StandardScaler
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StandardScalerOp extends OpNode[MleapContext, StandardScaler, StandardScalerModel] {
  override val Model: OpModel[MleapContext, StandardScalerModel] = new OpModel[MleapContext, StandardScalerModel] {
    override val klazz: Class[StandardScalerModel] = classOf[StandardScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(context: BundleContext[MleapContext], model: Model, obj: StandardScalerModel): Model = {
      model.withAttr("mean", obj.mean.map(_.toArray.toSeq).map(Value.doubleVector)).
        withAttr("std", obj.mean.map(_.toArray.toSeq).map(Value.doubleVector))
    }

    override def load(context: BundleContext[MleapContext], model: Model): StandardScalerModel = {
      val mean = model.getValue("mean").map(_.getDoubleVector.toArray).map(Vectors.dense)
      val std = model.getValue("std").map(_.getDoubleVector.toArray).map(Vectors.dense)
      StandardScalerModel(mean = mean, std = std)
    }
  }

  override val klazz: Class[StandardScaler] = classOf[StandardScaler]

  override def name(node: StandardScaler): String = node.uid

  override def model(node: StandardScaler): StandardScalerModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: StandardScalerModel): StandardScaler = {
    StandardScaler(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StandardScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
