package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MaxAbsScalerOp extends OpNode[MleapContext, MaxAbsScaler, MaxAbsScalerModel]{
  override val Model: OpModel[MleapContext, MaxAbsScalerModel] = new OpModel[MleapContext, MaxAbsScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(context: BundleContext[MleapContext], model: Model, obj: MaxAbsScalerModel): Model = {
      model.withAttr("maxAbs", Value.doubleVector(obj.maxAbs.toArray))
    }

    override def load(context: BundleContext[MleapContext], model: Model): MaxAbsScalerModel = {
      MaxAbsScalerModel(maxAbs = Vectors.dense(model.value("maxAbs").getDoubleVector.toArray))
    }
  }

  override def name(node: MaxAbsScaler): String = node.uid

  override def model(node: MaxAbsScaler): MaxAbsScalerModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: MaxAbsScalerModel): MaxAbsScaler = {
    MaxAbsScaler(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: MaxAbsScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
