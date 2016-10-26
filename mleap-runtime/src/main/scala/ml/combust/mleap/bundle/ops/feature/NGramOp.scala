package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.NGram

/**
  * Created by mikhail on 10/16/16.
  */
class NGramOp extends OpNode[MleapContext, NGram, NGramModel]{
  override val Model: OpModel[MleapContext, NGramModel] = new OpModel[MleapContext, NGramModel] {
    override val klazz: Class[NGramModel] = classOf[NGramModel]

    override def opName: String = Bundle.BuiltinOps.feature.ngram

    override def store(context: BundleContext[MleapContext], model: Model, obj: NGramModel): Model = {
      model.withAttr(Attribute("n", Value.long(obj.n)))
    }

    override def load(context: BundleContext[MleapContext], model: Model): NGramModel = {
      NGramModel(n = model.value("n").getLong.toInt)
    }
  }

  override val klazz: Class[NGram] = classOf[NGram]

  override def name(node: NGram): String = node.uid

  override def model(node: NGram): NGramModel = node.model

  override def load(context: BundleContext[MleapContext], node: Node, model: NGramModel): NGram = {
    NGram(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: NGram): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
