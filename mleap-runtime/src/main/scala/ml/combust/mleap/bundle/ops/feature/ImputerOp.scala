package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Imputer

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends MleapOp[Imputer, ImputerModel] {
  override val Model: OpModel[MleapContext, ImputerModel] = new OpModel[MleapContext, ImputerModel] {
    override val klazz: Class[ImputerModel] = classOf[ImputerModel]

    override def opName: String = Bundle.BuiltinOps.feature.imputer

    override def store(model: Model, obj: ImputerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
        model.withValue("surrogate_value", Value.double(obj.surrogateValue)).
          withValue("missing_value", Value.double(obj.missingValue)).
          withValue("strategy", Value.string(obj.strategy)).
          withValue("nullable_input", Value.boolean(obj.nullableInput))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): ImputerModel = {
      val missingValue = model.getValue("missing_value")
        .map(value => value.getDouble)
        .getOrElse(Double.NaN)

      // defaults to true for Scikit-learn serialized models
      val nullableInput = model.getValue("nullable_input")
          .map(value => value.getBoolean)
            .getOrElse(true)

      ImputerModel(model.value("surrogate_value").getDouble,
        missingValue,
        model.value("strategy").getString,
        nullableInput)
    }

  }

  override def model(node: Imputer): ImputerModel = node.model
}
