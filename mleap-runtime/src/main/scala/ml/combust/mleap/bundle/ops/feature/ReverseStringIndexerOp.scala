package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.transformer.feature.ReverseStringIndexer
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.types.{DataShape, ScalarShape}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 8/24/16.
  */
class ReverseStringIndexerOp extends MleapOp[ReverseStringIndexer, ReverseStringIndexerModel] {
  override val Model: OpModel[MleapContext, ReverseStringIndexerModel] = new OpModel[MleapContext, ReverseStringIndexerModel] {
    override val klazz: Class[ReverseStringIndexerModel] = classOf[ReverseStringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.reverse_string_indexer

    override def store(model: Model, obj: ReverseStringIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("labels", Value.stringList(obj.labels)).
        withValue("input_shape", Value.dataShape(obj.inputShape))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ReverseStringIndexerModel = {
      val shape = model.getValue("input_shape").map(_.getDataShape: DataShape).getOrElse(ScalarShape(false))
      ReverseStringIndexerModel(labels = model.value("labels").getStringList, shape)
    }
  }

  override def model(node: ReverseStringIndexer): ReverseStringIndexerModel = node.model
}
