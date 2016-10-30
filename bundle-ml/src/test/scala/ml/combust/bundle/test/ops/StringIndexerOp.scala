package ml.combust.bundle.test.ops

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.{BundleContext, dsl}

/**
  * Created by hollinwilkins on 8/21/16.
  */
case class StringIndexerModel(strings: Seq[String])
case class StringIndexer(uid: String,
                         input: String,
                         output: String,
                         model: StringIndexerModel) extends Transformer

class StringIndexerOp extends OpNode[Any, StringIndexer, StringIndexerModel] {
  override val Model: OpModel[Any, StringIndexerModel] = new OpModel[Any, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(context: BundleContext[Any], model: Model, obj: StringIndexerModel): Model = {
      model.withAttr(Attribute("labels", Value.stringList(obj.strings)))
    }

    override def load(context: BundleContext[Any], model: Model): StringIndexerModel = {
      StringIndexerModel(strings = model.value("labels").getStringList)
    }
  }

  override val klazz: Class[StringIndexer] = classOf[StringIndexer]

  override def name(node: StringIndexer): String = node.uid

  override def model(node: StringIndexer): StringIndexerModel = node.model

  override def load(context: BundleContext[Any], node: dsl.Node, model: StringIndexerModel): StringIndexer = {
    StringIndexer(uid = node.name,
      input = node.shape.standardInput.name,
      output = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: StringIndexer): Shape = Shape().withStandardIO(node.input, node.output)
}
