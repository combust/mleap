package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.NGramModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.NGram

/**
  * Created by mikhail on 10/16/16.
  */
class NGramOp extends MleapOp[NGram, NGramModel]{
  override val Model: OpModel[MleapContext, NGramModel] = new OpModel[MleapContext, NGramModel] {
    override val klazz: Class[NGramModel] = classOf[NGramModel]

    override def opName: String = Bundle.BuiltinOps.feature.ngram

    override def store(model: Model, obj: NGramModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("n", Value.long(obj.n))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): NGramModel = {
      NGramModel(n = model.value("n").getLong.toInt)
    }
  }

  override def model(node: NGram): NGramModel = node.model
}
