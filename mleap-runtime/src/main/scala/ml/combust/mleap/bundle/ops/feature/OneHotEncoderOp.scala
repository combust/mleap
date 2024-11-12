package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MultiInOutMleapOp
import ml.combust.mleap.core.feature.{HandleInvalid, OneHotEncoderModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.OneHotEncoder

/**
  * Created by hollinwilkins on 10/24/16.
  */
class OneHotEncoderOp extends MultiInOutMleapOp[OneHotEncoder, OneHotEncoderModel] {
  override val Model: OpModel[MleapContext, OneHotEncoderModel] = new OpModel[MleapContext, OneHotEncoderModel] {
    override val klazz: Class[OneHotEncoderModel] = classOf[OneHotEncoderModel]

    override def opName: String = Bundle.BuiltinOps.feature.one_hot_encoder

    override def store(model: Model, obj: OneHotEncoderModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model
        .withValue("category_sizes", Value.intList(obj.categorySizes))
        .withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))
        .withValue("drop_last", Value.boolean(obj.dropLast))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneHotEncoderModel = {
      val dropLast = model.value("drop_last").getBoolean
      if (model.getValue("size").nonEmpty) {
        // Old version of 1HE.
        OneHotEncoderModel(
          categorySizes = Array(model.value("size").getLong.toInt),
          handleInvalid =
            model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString(_, false))
              //this maintains backwards compatibility for models that don't have handle_invalid serialized
              .getOrElse(if (dropLast) HandleInvalid.Keep else HandleInvalid.default),
          dropLast = dropLast)
      } else {
        // New version of 1HE.
        OneHotEncoderModel(
          categorySizes = model.value("category_sizes").getIntList.toArray,
          handleInvalid = HandleInvalid.fromString(model.value("handle_invalid").getString, false),
          dropLast = dropLast)
      }
    }
  }

  override def model(node: OneHotEncoder): OneHotEncoderModel = node.model

}
