package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StringIndexerOp extends OpNode[MleapContext, StringIndexer, StringIndexerModel] {
  override val Model: OpModel[MleapContext, StringIndexerModel] = new OpModel[MleapContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("labels", Value.stringList(obj.labels)).
        withAttr("handle_invalid", Value.string(obj.handleInvalid.asParamString))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringIndexerModel = {
      StringIndexerModel(labels = model.value("labels").getStringList,
        handleInvalid = HandleInvalid.fromString(model.value("handle_invalid").getString))
    }
  }

  override val klazz: Class[StringIndexer] = classOf[StringIndexer]

  override def name(node: StringIndexer): String = node.uid

  override def model(node: StringIndexer): StringIndexerModel = node.model

  override def load(node: Node, model: StringIndexerModel)
                   (implicit context: BundleContext[MleapContext]): StringIndexer = {
    StringIndexer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
