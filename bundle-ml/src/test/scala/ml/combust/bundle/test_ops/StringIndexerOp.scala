package ml.combust.bundle.test_ops

import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl.{Bundle, _}

/**
  * Created by hollinwilkins on 8/21/16.
  */
case class StringIndexerModel(strings: Seq[String])
case class StringIndexer(uid: String,
                         input: String,
                         output: String,
                         model: StringIndexerModel) extends Transformer

object StringIndexerOp extends OpNode[StringIndexer, StringIndexerModel] {
  override val Model: OpModel[StringIndexerModel] = new OpModel[StringIndexerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(context: BundleContext, model: WritableModel, obj: StringIndexerModel): WritableModel = {
      model.withAttr(Attribute("labels", Value.stringList(obj.strings)))
    }

    override def load(context: BundleContext, model: ReadableModel): StringIndexerModel = {
      StringIndexerModel(strings = model.value("labels").getStringList)
    }
  }

  override def name(node: StringIndexer): String = node.uid

  override def model(node: StringIndexer): StringIndexerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: StringIndexerModel): StringIndexer = {
    StringIndexer(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringIndexer): Shape = Shape().withStandardIO(node.input, node.output)
}
