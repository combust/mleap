package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Imputer

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends OpNode[MleapContext, Imputer, ImputerModel] {
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

      ImputerModel(model.value("surrogate_value").getDouble,
        missingValue,
        model.value("strategy").getString,
        nullableInput = model.value("nullable_input").getBoolean)
    }

  }

  override val klazz: Class[Imputer] = classOf[Imputer]

  override def name(node: Imputer): String = node.uid

  override def model(node: Imputer): ImputerModel = node.model

  override def load(node: Node, model: ImputerModel)
                   (implicit context: BundleContext[MleapContext]): Imputer = {
    Imputer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Imputer): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
