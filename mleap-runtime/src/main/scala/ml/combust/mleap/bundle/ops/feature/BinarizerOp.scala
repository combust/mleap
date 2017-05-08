package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Binarizer
import ml.combust.mleap.runtime.types.BundleTypeConverters._
import ml.combust.mleap.runtime.types.DataType

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerOp extends OpNode[MleapContext, Binarizer, BinarizerModel] {
  var inputDataType: Option[DataType] = None
  var outputDataType: Option[DataType] = None

  override val Model: OpModel[MleapContext, BinarizerModel] = new OpModel[MleapContext, BinarizerModel] {

    override val klazz: Class[BinarizerModel] = classOf[BinarizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.binarizer

    override def store(model: Model, obj: BinarizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("threshold", Value.double(obj.threshold))
            .withAttr("input_types", Value.dataType(mleapTypeToBundleType(inputDataType.get)))
            .withAttr("output_types",  Value.dataType(mleapTypeToBundleType(outputDataType.get)))

    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BinarizerModel = {
      inputDataType = getDataType(model, "input_types")
      outputDataType = getDataType(model, "output_types")
      BinarizerModel(model.value("threshold").getDouble)
    }

    private def getDataType(model: Model, colName: String): Option[DataType] = {
      model.attributes match {
        case None => None
        case Some(attributeList) => attributeList.get(colName) match {
          case None => None
          case Some(attribute) => Some(attribute.value.getDataType)
        }
      }
    }
  }

  override val klazz: Class[Binarizer] = classOf[Binarizer]

  override def name(node: Binarizer): String = node.uid

  override def model(node: Binarizer): BinarizerModel = {
    inputDataType = node.inputDataType
    outputDataType = node.outputDataType
    node.model
  }

  override def load(node: Node, model: BinarizerModel)
                   (implicit context: BundleContext[MleapContext]): Binarizer = {
    Binarizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      inputDataType = inputDataType,
      outputCol = node.shape.standardOutput.name,
      outputDataType = outputDataType,
      model = model)
  }

  override def shape(node: Binarizer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
