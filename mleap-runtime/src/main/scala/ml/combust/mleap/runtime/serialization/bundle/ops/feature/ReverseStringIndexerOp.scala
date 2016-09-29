package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.transformer.feature.ReverseStringIndexer
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._

/**
  * Created by hollinwilkins on 8/24/16.
  */
object ReverseStringIndexerOp extends OpNode[ReverseStringIndexer, ReverseStringIndexerModel] {
  override val Model: OpModel[ReverseStringIndexerModel] = new OpModel[ReverseStringIndexerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(context: BundleContext, model: WritableModel, obj: ReverseStringIndexerModel): WritableModel = {
      model.withAttr(Attribute("labels", Value.stringList(obj.labels)))
    }

    override def load(context: BundleContext, model: ReadableModel): ReverseStringIndexerModel = {
      ReverseStringIndexerModel(labels = model.value("labels").getStringList)
    }
  }

  override def name(node: ReverseStringIndexer): String = node.uid

  override def model(node: ReverseStringIndexer): ReverseStringIndexerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: ReverseStringIndexerModel): ReverseStringIndexer = {
    ReverseStringIndexer(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: ReverseStringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
