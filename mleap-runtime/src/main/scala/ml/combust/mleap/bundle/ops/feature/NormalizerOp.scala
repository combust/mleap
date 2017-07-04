package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Normalizer

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerOp extends MleapOp[Normalizer, NormalizerModel] {
  override val Model: OpModel[MleapContext, NormalizerModel] = new OpModel[MleapContext, NormalizerModel] {
    override val klazz: Class[NormalizerModel] = classOf[NormalizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.normalizer

    override def store(model: Model, obj: NormalizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("p_norm", Value.double(obj.pNorm))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): NormalizerModel = {
      NormalizerModel(pNorm = model.value("p_norm").getDouble)
    }
  }

  override def model(node: Normalizer): NormalizerModel = node.model
}
