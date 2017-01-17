package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.StringMapModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StringMap

/**
  * Created by hollinwilkins on 1/5/17.
  */
class StringMapOp extends OpNode[MleapContext, StringMap, StringMapModel] {
  override val Model: OpModel[MleapContext, StringMapModel] = new OpModel[MleapContext, StringMapModel] {
    override val klazz: Class[StringMapModel] = classOf[StringMapModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_map

    override def store(model: Model, obj: StringMapModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (labels, values) = obj.labels.toSeq.unzip

      model.withAttr("labels", Value.stringList(labels)).
        withAttr("values", Value.doubleList(values))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringMapModel = {
      val labels = model.value("labels").getStringList
      val values = model.value("values").getDoubleList

      StringMapModel(labels.zip(values).toMap)
    }
  }

  override val klazz: Class[StringMap] = classOf[StringMap]

  override def name(node: StringMap): String = node.uid

  override def model(node: StringMap): StringMapModel = node.model

  override def load(node: Node, model: StringMapModel)
                   (implicit context: BundleContext[MleapContext]): StringMap = {
    StringMap(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringMap): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
