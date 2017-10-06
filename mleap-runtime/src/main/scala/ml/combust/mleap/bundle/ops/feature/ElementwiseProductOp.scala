package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.ElementwiseProductModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/23/16.
  */
class ElementwiseProductOp extends MleapOp[ElementwiseProduct, ElementwiseProductModel] {
  override val Model: OpModel[MleapContext, ElementwiseProductModel] = new OpModel[MleapContext, ElementwiseProductModel] {
    override val klazz: Class[ElementwiseProductModel] = classOf[ElementwiseProductModel]

    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(model: Model, obj: ElementwiseProductModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("scaling_vec", Value.vector(obj.scalingVec.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ElementwiseProductModel = {
      ElementwiseProductModel(scalingVec = Vectors.dense(model.value("scaling_vec").getTensor[Double].toArray))
    }
  }

  override def model(node: ElementwiseProduct): ElementwiseProductModel = node.model
}
