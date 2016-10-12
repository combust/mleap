package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/22/16.
  */
object StringIndexerOp extends OpNode[StringIndexer, StringIndexerModel] {
  override val Model: OpModel[StringIndexerModel] = new OpModel[StringIndexerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(context: BundleContext, model: Model, obj: StringIndexerModel): Model = {
      model.withAttr(Attribute("labels", Value.stringList(obj.labels)))
    }

    override def load(context: BundleContext, model: Model): StringIndexerModel = {
      StringIndexerModel(labels = model.value("labels").getStringList)
    }
  }

  override def name(node: StringIndexer): String = node.uid

  override def model(node: StringIndexer): StringIndexerModel = node.model

  override def load(context: BundleContext, node: Node, model: StringIndexerModel): StringIndexer = {
    StringIndexer(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
