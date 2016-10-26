package ml.combust.mleap.bundle.ops.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.transformer.feature.ReverseStringIndexer
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/24/16.
  */
class ReverseStringIndexerOp extends OpNode[MleapContext, ReverseStringIndexer, ReverseStringIndexerModel] {
  override val Model: OpModel[MleapContext, ReverseStringIndexerModel] = new OpModel[MleapContext, ReverseStringIndexerModel] {
    override val klazz: Class[ReverseStringIndexerModel] = classOf[ReverseStringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(context: BundleContext[MleapContext], model: Model, obj: ReverseStringIndexerModel): Model = {
      model.withAttr("labels", Value.stringList(obj.labels))
    }

    override def load(context: BundleContext[MleapContext], model: Model): ReverseStringIndexerModel = {
      ReverseStringIndexerModel(labels = model.value("labels").getStringList)
    }
  }

  override val klazz: Class[ReverseStringIndexer] = classOf[ReverseStringIndexer]

  override def name(node: ReverseStringIndexer): String = node.uid

  override def model(node: ReverseStringIndexer): ReverseStringIndexerModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: ReverseStringIndexerModel): ReverseStringIndexer = {
    ReverseStringIndexer(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: ReverseStringIndexer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
