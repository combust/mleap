package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.OneHotEncoder

/**
  * Created by hollinwilkins on 10/24/16.
  */
class OneHotEncoderOp extends MleapOp[OneHotEncoder, OneHotEncoderModel] {
  override val Model: OpModel[MleapContext, OneHotEncoderModel] = new OpModel[MleapContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("size", Value.long(obj.size)).
        withValue("drop_last", Value.boolean(obj.dropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneHotEncoderModel = {
      OneHotEncoderModel(size = model.value("size").getLong.toInt,
        dropLast = model.value("drop_last").getBoolean)
    }
  }

  override def model(node: OneHotEncoder): OneHotEncoderModel = node.model
}
