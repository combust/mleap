package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StringIndexerOp extends OpNode[MleapContext, StringIndexer, StringIndexerModel] {
  var inputDataType: Option[DataType] = None

  override val Model: OpModel[MleapContext, StringIndexerModel] = new OpModel[MleapContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      inputDataType.map(inputType => model.withAttr("input_types", Value.dataType(mleapTypeToBundleType(inputType))))
      .getOrElse(model)
        .withAttr("labels", Value.stringList(obj.labels))
        .withAttr("handle_invalid", Value.string(obj.handleInvalid.asParamString))

    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringIndexerModel = {
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString).getOrElse(HandleInvalid.default)

      inputDataType = model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get("input_types") match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataType)
        }
      }

      StringIndexerModel(labels = model.value("labels").getStringList,
        handleInvalid = handleInvalid)
    }
  }

  override val klazz: Class[StringIndexer] = classOf[StringIndexer]

  override def name(node: StringIndexer): String = node.uid

  override def model(node: StringIndexer): StringIndexerModel = {
    inputDataType = node.inputDataType
    node.model
  }

  override def load(node: Node, model: StringIndexerModel)
                   (implicit context: BundleContext[MleapContext]): StringIndexer = {
    StringIndexer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      inputDataType = inputDataType,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
