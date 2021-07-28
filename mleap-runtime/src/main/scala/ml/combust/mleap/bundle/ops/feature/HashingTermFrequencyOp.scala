package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.HashingTermFrequencyModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.HashingTermFrequency

/**
  * Created by hollinwilkins on 10/30/16.
  */
class HashingTermFrequencyOp extends MleapOp[HashingTermFrequency, HashingTermFrequencyModel] {
  override val Model: OpModel[MleapContext, HashingTermFrequencyModel] = new OpModel[MleapContext, HashingTermFrequencyModel] {
    override val klazz: Class[HashingTermFrequencyModel] = classOf[HashingTermFrequencyModel]

    override def opName: String = Bundle.BuiltinOps.feature.hashing_term_frequency

    override def store(model: Model, obj: HashingTermFrequencyModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("num_features", Value.long(obj.numFeatures))
        .withValue("binary", Value.boolean(obj.binary)).
        withValue("version", Value.int(obj.version))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): HashingTermFrequencyModel = {
      HashingTermFrequencyModel(
        numFeatures = model.value("num_features").getLong.toInt,
        binary = model.getValue("binary").map(_.getBoolean).getOrElse(false),
        version = model.getValue("version").map(_.getInt).getOrElse(1)
      )
    }
  }

  override def model(node: HashingTermFrequency): HashingTermFrequencyModel = node.model
}
