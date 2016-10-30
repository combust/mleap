package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.Normalizer

/**
  * Created by hollinwilkins on 9/24/16.
  */
class NormalizerOp extends OpNode[MleapContext, Normalizer, NormalizerModel] {
  override val Model: OpModel[MleapContext, NormalizerModel] = new OpModel[MleapContext, NormalizerModel] {
    override val klazz: Class[NormalizerModel] = classOf[NormalizerModel]

    override def opName: String = Bundle.BuiltinOps.feature.normalizer

    override def store(model: Model, obj: NormalizerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("p_norm", Value.double(obj.pNorm))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): NormalizerModel = {
      NormalizerModel(pNorm = model.value("p_norm").getDouble)
    }
  }

  override val klazz: Class[Normalizer] = classOf[Normalizer]

  override def name(node: Normalizer): String = node.uid

  override def model(node: Normalizer): NormalizerModel = node.model

  override def load(node: Node, model: NormalizerModel)
                   (implicit context: BundleContext[MleapContext]): Normalizer = {
    Normalizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Normalizer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
