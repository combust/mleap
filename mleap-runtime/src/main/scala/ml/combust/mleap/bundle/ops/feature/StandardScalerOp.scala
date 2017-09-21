package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.runtime.transformer.feature.StandardScaler
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StandardScalerOp extends MleapOp[StandardScaler, StandardScalerModel] {
  override val Model: OpModel[MleapContext, StandardScalerModel] = new OpModel[MleapContext, StandardScalerModel] {
    override val klazz: Class[StandardScalerModel] = classOf[StandardScalerModel]

    override def opName: String = Bundle.BuiltinOps.feature.standard_scaler

    override def store(model: Model, obj: StandardScalerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("mean", obj.mean.map(_.toArray).map(Value.vector[Double])).
        withValue("std", obj.mean.map(_.toArray).map(Value.vector[Double]))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StandardScalerModel = {
      val mean = model.getValue("mean").map(_.getTensor[Double].toArray).map(Vectors.dense)
      val std = model.getValue("std").map(_.getTensor[Double].toArray).map(Vectors.dense)
      StandardScalerModel(mean = mean, std = std)
    }
  }

  override def model(node: StandardScaler): StandardScalerModel = node.model
}
