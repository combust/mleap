package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.runtime.transformer.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MaxAbsScalerOp extends OpNode[MaxAbsScaler, MaxAbsScalerModel]{
  override val Model: OpModel[MaxAbsScalerModel] = new OpModel[MaxAbsScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(context: BundleContext, model: WritableModel, obj: MaxAbsScalerModel): WritableModel = {
      model.withAttr(Attribute("maxAbs", Value.doubleVector(obj.maxAbs.toArray)))
    }

    override def load(context: BundleContext, model: ReadableModel): MaxAbsScalerModel = {
      MaxAbsScalerModel(maxAbs = Vectors.dense(model.value("maxAbs").getDoubleVector.toArray))
    }

  }

  override def name(node: MaxAbsScaler): String = node.uid

  override def model(node: MaxAbsScaler): MaxAbsScalerModel = node.model


  override def load(context: BundleContext, node: ReadableNode, model: MaxAbsScalerModel): MaxAbsScaler = {
    MaxAbsScaler(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: MaxAbsScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
