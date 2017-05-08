package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Imputer
import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by mikhail on 12/18/16.
  */
class ImputerOp extends OpNode[MleapContext, Imputer, ImputerModel] {
  var inputDataType: Option[DataType] = None

  override val Model: OpModel[MleapContext, ImputerModel] = new OpModel[MleapContext, ImputerModel] {
    override val klazz: Class[ImputerModel] = classOf[ImputerModel]

    override def opName: String = Bundle.BuiltinOps.feature.imputer

    override def store(model: Model, obj: ImputerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("surrogate_value", Value.double(obj.surrogateValue)).
        withAttr("missing_value", Value.double(obj.missingValue)).
        withAttr("strategy", Value.string(obj.strategy))
        .withAttr("input_types", Value.dataType(inputDataType.get))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): ImputerModel = {
      inputDataType = model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get("input_types") match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataType)
        }
      }

      ImputerModel(model.value("surrogate_value").getDouble,
        model.value("missing_value").getDouble,
        model.value("strategy").getString)
    }

  }

  override val klazz: Class[Imputer] = classOf[Imputer]

  override def name(node: Imputer): String = node.uid

  override def model(node: Imputer): ImputerModel = {
    inputDataType = node.inputDataType
    node.model
  }


  override def load(node: Node, model: ImputerModel)
                   (implicit context: BundleContext[MleapContext]): Imputer = {
    Imputer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      inputDataType = inputDataType,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Imputer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
