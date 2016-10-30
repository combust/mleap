package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.BucketizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Bucketizer

/**
  * Created by mikhail on 9/19/16.
  */
class BucketizerOp extends OpNode[MleapContext, Bucketizer, BucketizerModel]{
  override val Model: OpModel[MleapContext, BucketizerModel] = new OpModel[MleapContext, BucketizerModel] {
    override val klazz: Class[BucketizerModel] = classOf[BucketizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(model: Model, obj: BucketizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("splits", Value.doubleList(obj.splits))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BucketizerModel = {
      BucketizerModel(splits = model.value("splits").getDoubleList.toArray)
    }
  }

  override val klazz: Class[Bucketizer] = classOf[Bucketizer]

  override def name(node: Bucketizer): String = node.uid

  override def model(node: Bucketizer): BucketizerModel = node.model

  override def load(node: Node, model: BucketizerModel)
                   (implicit context: BundleContext[MleapContext]): Bucketizer = {
    Bucketizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Bucketizer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
