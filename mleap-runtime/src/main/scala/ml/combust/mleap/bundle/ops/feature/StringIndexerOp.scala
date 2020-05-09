package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.runtime.transformer.feature.StringIndexer
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StringIndexerOp extends MleapOp[StringIndexer, StringIndexerModel] {
  override val Model: OpModel[MleapContext, StringIndexerModel] = new OpModel[MleapContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
        model.withValue("labels", Value.stringList(obj.labels)).
          withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))

    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringIndexerModel = {
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString(_)).getOrElse(HandleInvalid.default)

      StringIndexerModel(labels = model.value("labels").getStringList,
        handleInvalid = handleInvalid)
    }
  }

  override def model(node: StringIndexer): StringIndexerModel = node.model
}
