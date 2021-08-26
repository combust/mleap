package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Binarizer
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerOp extends MleapOp[Binarizer, BinarizerModel] {
  override val Model: OpModel[MleapContext, BinarizerModel] = new OpModel[MleapContext, BinarizerModel] {

    override val klazz: Class[BinarizerModel] = classOf[BinarizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.binarizer

    override def store(model: Model, obj: BinarizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
        model.withValue("threshold", Value.double(obj.threshold)).
          withValue("input_shapes", Value.dataShape(obj.inputShape))
      }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BinarizerModel = {
      val threshold: Option[Double] = model.getValue("threshold").map(_.getDouble)
      val thresholds: Option[Seq[Double]] = model.getValue("thresholds").map(_.getDoubleList)
      (threshold, thresholds) match {
        case (None, None) => throw new IllegalArgumentException("Neither threshold nor thresholds were found")
        case (Some(v), None) => BinarizerModel(v, model.value("input_shapes").getDataShape)
        case (None, Some(v)) => throw new UnsupportedOperationException("Multi-column Binarizer not supported yet")
        case (_, _) => throw new IllegalArgumentException("Both thresholds and threshold were found")
      }
    }
  }

  override def model(node: Binarizer): BinarizerModel = node.model
}
