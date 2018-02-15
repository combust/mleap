package ml.combust.mleap.bundle.ops.sklearn

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.BinarizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Binarizer
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * This is needed because the sklearn Binarizer outputs a input_shapes list
  * instead of just a single input_shape as Spark.
  */
class BinarizerOp extends MleapOp[Binarizer, BinarizerModel] {
  override val Model: OpModel[MleapContext, BinarizerModel] = new OpModel[MleapContext, BinarizerModel] {

    override val klazz: Class[BinarizerModel] = classOf[BinarizerModel]

    override def opName: String = "sklearn_binarizer"

    override def store(model: Model, obj: BinarizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("threshold", Value.double(obj.threshold)).
        withValue("input_shapes", Value.dataShapeList(Seq(obj.inputShape).map(mleapToBundleShape)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BinarizerModel = {
      val inputShapes = model.value("input_shapes").getDataShapeList.map(bundleToMleapShape)

      BinarizerModel(model.value("threshold").getDouble, inputShapes(0))
    }
  }

  override def model(node: Binarizer): BinarizerModel = node.model
}
