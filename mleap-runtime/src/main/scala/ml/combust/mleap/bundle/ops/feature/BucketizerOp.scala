package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.{BucketizerModel, HandleInvalid}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Bucketizer
import ml.combust.mleap.runtime.transformer.feature.BucketizerUtil._

/**
  * Created by mikhail on 9/19/16.
  */
class BucketizerOp extends MleapOp[Bucketizer, BucketizerModel]{
  override val Model: OpModel[MleapContext, BucketizerModel] = new OpModel[MleapContext, BucketizerModel] {
    override val klazz: Class[BucketizerModel] = classOf[BucketizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(model: Model, obj: BucketizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("splits", Value.doubleList(obj.splits)).
        withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BucketizerModel = {
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString).getOrElse(HandleInvalid.default)

      BucketizerModel(splits = restoreSplits(model.value("splits").getDoubleList.toArray),
        handleInvalid = handleInvalid)
    }
  }

  override def model(node: Bucketizer): BucketizerModel = node.model
}
