package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.ElementwiseProductModel
import ml.combust.mleap.runtime.transformer.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by mikhail on 9/23/16.
  */
object ElementwiseProductOp extends OpNode[ElementwiseProduct, ElementwiseProductModel] {
  override val Model: OpModel[ElementwiseProductModel] = new OpModel[ElementwiseProductModel] {
    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(context: BundleContext, model: WritableModel, obj: ElementwiseProductModel): WritableModel = {
      model.withAttr(Attribute("scalingVec", Value.doubleVector(obj.scalingVec.toArray)))
    }

    override def load(context: BundleContext, model: ReadableModel): ElementwiseProductModel = {
      ElementwiseProductModel(scalingVec = Vectors.dense(model.value("scalingVec").getDoubleVector.toArray))
    }

  }

  override def name(node: ElementwiseProduct): String = node.uid

  override def model(node: ElementwiseProduct): ElementwiseProductModel = node.model


  override def load(context: BundleContext, node: ReadableNode, model: ElementwiseProductModel): ElementwiseProduct = {
    ElementwiseProduct(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model
    )
  }

  override def shape(node: ElementwiseProduct): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)

}
