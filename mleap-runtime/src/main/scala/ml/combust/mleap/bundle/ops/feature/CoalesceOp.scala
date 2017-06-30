package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types.DataType
import ml.combust.mleap.runtime.{MleapContext, types}
import ml.combust.mleap.runtime.transformer.feature.Coalesce
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 1/5/17.
  */
class CoalesceOp extends OpNode[MleapContext, Coalesce, CoalesceModel] {

  var inputDataTypes: Option[Array[DataType]] = None

  override val Model: OpModel[MleapContext, CoalesceModel] = new OpModel[MleapContext, CoalesceModel] {
    override val klazz: Class[CoalesceModel] = classOf[CoalesceModel]

    override def opName: String = Bundle.BuiltinOps.feature.coalesce

    override def store(model: Model, obj: CoalesceModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      inputDataTypes.map(inputTypes => model.withAttr("input_types", Value.dataTypeList(
        inputTypes.toSeq.map(dataType => mleapTypeToBundleType(dataType))))).getOrElse(model)
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): CoalesceModel = {
      inputDataTypes = model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get("input_types") match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataTypeList.map(v => v: DataType).toArray)
        }
      }

      CoalesceModel()
    }
  }

  override val klazz: Class[Coalesce] = classOf[Coalesce]

  override def name(node: Coalesce): String = node.uid

  override def model(node: Coalesce): CoalesceModel = {
    inputDataTypes = node.inputDataTypes
    node.model
  }

  override def load(node: Node, model: CoalesceModel)
                   (implicit context: BundleContext[MleapContext]): Coalesce = {
    Coalesce(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      inputDataTypes = inputDataTypes,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Coalesce): Shape = {
    var i = 0
    node.inputCols.foldLeft(Shape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.outputCol)
  }
}
