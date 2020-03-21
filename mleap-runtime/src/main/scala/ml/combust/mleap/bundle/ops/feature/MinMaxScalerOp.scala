package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.MinMaxScalerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MinMaxScalerOp extends MleapOp[MinMaxScaler, MinMaxScalerModel]{
  override val Model: OpModel[MleapContext, MinMaxScalerModel] = new OpModel[MleapContext, MinMaxScalerModel] {
    override val klazz: Class[MinMaxScalerModel] = classOf[MinMaxScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_max_scaler

    override def store(model: Model, obj: MinMaxScalerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("min", Value.vector(obj.originalMin.toArray)).
        withValue("max", Value.vector(obj.originalMax.toArray))
        .withValue("minValue", Value.double(obj.minValue))
        .withValue("maxValue", Value.double(obj.maxValue))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MinMaxScalerModel = {
      val minValue = model.getValue("minValue").map(_.getDouble).getOrElse(0.0)
      val maxValue = model.getValue("maxValue").map(_.getDouble).getOrElse(1.0)

      MinMaxScalerModel(originalMin = Vectors.dense(model.value("min").getTensor[Double].toArray),
        originalMax = Vectors.dense(model.value("max").getTensor[Double].toArray),
        minValue = minValue,
        maxValue = maxValue
      )
    }
  }

  override def model(node: MinMaxScaler): MinMaxScalerModel = node.model
}
