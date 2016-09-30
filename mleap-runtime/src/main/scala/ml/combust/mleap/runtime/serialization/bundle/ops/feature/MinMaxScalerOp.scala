package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.MinMaxScalerModel
import ml.combust.mleap.runtime.transformer.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
object MinMaxScalerOp extends OpNode[MinMaxScaler, MinMaxScalerModel]{
  override val Model: OpModel[MinMaxScalerModel] = new OpModel[MinMaxScalerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(context: BundleContext, model: Model, obj: MinMaxScalerModel): Model = {
      model.withAttr(Attribute("min", Value.doubleVector(obj.originalMin.toArray))).
        withAttr(Attribute("max", Value.doubleVector(obj.originalMax.toArray)))
    }

    override def load(context: BundleContext, model: Model): MinMaxScalerModel = {
      MinMaxScalerModel(originalMin = Vectors.dense(model.value("min").getDoubleVector.toArray),
        originalMax = Vectors.dense(model.value("max").getDoubleVector.toArray))
    }
  }

  override def name(node: MinMaxScaler): String = node.uid

  override def model(node: MinMaxScaler): MinMaxScalerModel = node.model

  override def load(context: BundleContext, node: Node, model: MinMaxScalerModel): MinMaxScaler = {
    MinMaxScaler(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: MinMaxScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
