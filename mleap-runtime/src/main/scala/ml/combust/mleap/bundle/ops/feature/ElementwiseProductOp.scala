package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.ElementwiseProductModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/23/16.
  */
class ElementwiseProductOp extends OpNode[MleapContext, ElementwiseProduct, ElementwiseProductModel] {
  override val Model: OpModel[MleapContext, ElementwiseProductModel] = new OpModel[MleapContext, ElementwiseProductModel] {
    override val klazz: Class[ElementwiseProductModel] = classOf[ElementwiseProductModel]

    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(model: Model, obj: ElementwiseProductModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("scalingVec", Value.doubleVector(obj.scalingVec.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ElementwiseProductModel = {
      ElementwiseProductModel(scalingVec = Vectors.dense(model.value("scalingVec").getDoubleVector.toArray))
    }
  }

  override val klazz: Class[ElementwiseProduct] = classOf[ElementwiseProduct]

  override def name(node: ElementwiseProduct): String = node.uid

  override def model(node: ElementwiseProduct): ElementwiseProductModel = node.model

  override def load(node: Node, model: ElementwiseProductModel)
                   (implicit context: BundleContext[MleapContext]): ElementwiseProduct = {
    ElementwiseProduct(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model
    )
  }

  override def shape(node: ElementwiseProduct): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)

}
