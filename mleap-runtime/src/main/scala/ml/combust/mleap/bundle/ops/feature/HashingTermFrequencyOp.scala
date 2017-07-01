package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.HashingTermFrequency

/**
  * Created by hollinwilkins on 10/30/16.
  */
class HashingTermFrequencyOp extends OpNode[MleapContext, HashingTermFrequency, HashingTermFrequencyModel] {
  override val Model: OpModel[MleapContext, HashingTermFrequencyModel] = new OpModel[MleapContext, HashingTermFrequencyModel] {
    override val klazz: Class[HashingTermFrequencyModel] = classOf[HashingTermFrequencyModel]

    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(model: Model, obj: HashingTermFrequencyModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): HashingTermFrequencyModel = {
      HashingTermFrequencyModel(numFeatures = model.value("num_features").getLong.toInt)
    }
  }

  override val klazz: Class[HashingTermFrequency] = classOf[HashingTermFrequency]

  override def name(node: HashingTermFrequency): String = node.uid

  override def model(node: HashingTermFrequency): HashingTermFrequencyModel = node.model

  override def load(node: Node, model: HashingTermFrequencyModel)
                   (implicit context: BundleContext[MleapContext]): HashingTermFrequency = {
    HashingTermFrequency(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: HashingTermFrequency): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
