package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.runtime.{MleapContext, types}
import ml.combust.mleap.runtime.transformer.feature.Interaction
import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 4/26/17.
  */
class InteractionOp extends OpNode[MleapContext, Interaction, InteractionModel] {
  var inputDataTypes: Option[Array[DataType]] = None

  override val Model: OpModel[MleapContext, InteractionModel] = new OpModel[MleapContext, InteractionModel] {
    override val klazz: Class[InteractionModel] = classOf[InteractionModel]

    override def opName: String = Bundle.BuiltinOps.feature.interaction

    override def store(model: Model, obj: InteractionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val m = model.withAttr("input_types", Value.dataTypeList(
        inputDataTypes.get.toSeq.map(dataType => mleapTypeToBundleType(dataType))))
        .withAttr("num_inputs", Value.int(obj.featuresSpec.length))
      obj.featuresSpec.zipWithIndex.foldLeft(m) {
        case (m2, (numFeatures, index)) => m2.withAttr(s"num_features$index", Value.intList(numFeatures))
      }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): InteractionModel = {
      val numInputs = model.value("num_inputs").getInt
      val spec = (0 until numInputs).map {
        index => model.value(s"num_features$index").getIntList.toArray
      }.toArray

      inputDataTypes = model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get("input_types") match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataTypeList.map(v => v: types.DataType).toArray)
        }
      }

      InteractionModel(spec)
    }
  }

  override val klazz: Class[Interaction] = classOf[Interaction]

  override def name(node: Interaction): String = node.uid

  override def model(node: Interaction): InteractionModel = {
    inputDataTypes = node.inputDataTypes
    node.model
  }

  override def load(node: Node, model: InteractionModel)
                   (implicit context: BundleContext[MleapContext]): Interaction = {
    Interaction(uid = node.name,
      inputCols = node.shape.inputs.map(_.name).toArray,
      inputDataTypes = inputDataTypes,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Interaction): Shape = {
    val s = Shape().withStandardOutput(node.outputCol)
    node.inputCols.zipWithIndex.foldLeft(s) {
      case (s2, (input, index)) => s2.withInput(input, s"input$index")
    }
  }
}

