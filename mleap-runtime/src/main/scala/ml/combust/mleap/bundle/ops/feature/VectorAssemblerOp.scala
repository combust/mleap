package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.{MleapContext, types}
import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/22/16.
  */
class VectorAssemblerOp extends OpNode[MleapContext, VectorAssembler, VectorAssemblerModel] {

  var inputDataTypes: Option[Array[DataType]] = None

  override val Model: OpModel[MleapContext, VectorAssemblerModel] = new OpModel[MleapContext, VectorAssemblerModel] {
    override val klazz: Class[VectorAssemblerModel] = classOf[VectorAssemblerModel]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(model: Model, obj: VectorAssemblerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      inputDataTypes.map(inputTypes => model.withAttr("input_types", Value.dataTypeList(
        inputTypes.toSeq.map(dataType => mleapTypeToBundleType(dataType))))).getOrElse(model)
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): VectorAssemblerModel = {
      inputDataTypes = model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get("input_types") match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataTypeList.map(v => v: types.DataType).toArray)
        }
      }
      VectorAssemblerModel.default
    }
  }

  override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssemblerModel = {
    inputDataTypes = node.inputDataTypes
    VectorAssemblerModel.default
  }

  override def load(node: Node, model: VectorAssemblerModel)
                   (implicit context: BundleContext[MleapContext]): VectorAssembler = {
    VectorAssembler(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      inputDataTypes = inputDataTypes,
      outputCol = node.shape.standardOutput.name)
  }

  override def shape(node: VectorAssembler): Shape = {
    var i = 0
    node.inputCols.foldLeft(Shape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.outputCol)
  }
}
