package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.MinMaxScalerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MinMaxScalerOp extends OpNode[MleapContext, MinMaxScaler, MinMaxScalerModel]{
  override val Model: OpModel[MleapContext, MinMaxScalerModel] = new OpModel[MleapContext, MinMaxScalerModel] {
    override val klazz: Class[MinMaxScalerModel] = classOf[MinMaxScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(context: BundleContext[MleapContext], model: Model, obj: MinMaxScalerModel): Model = {
      model.withAttr("min", Value.doubleVector(obj.originalMin.toArray)).
        withAttr("max", Value.doubleVector(obj.originalMax.toArray))
    }

    override def load(context: BundleContext[MleapContext], model: Model): MinMaxScalerModel = {
      MinMaxScalerModel(originalMin = Vectors.dense(model.value("min").getDoubleVector.toArray),
        originalMax = Vectors.dense(model.value("max").getDoubleVector.toArray))
    }
  }

  override val klazz: Class[MinMaxScaler] = classOf[MinMaxScaler]

  override def name(node: MinMaxScaler): String = node.uid

  override def model(node: MinMaxScaler): MinMaxScalerModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: MinMaxScalerModel): MinMaxScaler = {
    MinMaxScaler(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: MinMaxScaler): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
