package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.MaxAbsScalerModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/19/16.
  */
class MaxAbsScalerOp extends MleapOp[MaxAbsScaler, MaxAbsScalerModel]{
  override val Model: OpModel[MleapContext, MaxAbsScalerModel] = new OpModel[MleapContext, MaxAbsScalerModel] {
    override val klazz: Class[MaxAbsScalerModel] = classOf[MaxAbsScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.max_abs_scaler

    override def store(model: Model, obj: MaxAbsScalerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("maxAbs", Value.vector(obj.maxAbs.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MaxAbsScalerModel = {
      MaxAbsScalerModel(maxAbs = Vectors.dense(model.value("maxAbs").getTensor[Double].toArray))
    }
  }

  override def model(node: MaxAbsScaler): MaxAbsScalerModel = node.model
}
