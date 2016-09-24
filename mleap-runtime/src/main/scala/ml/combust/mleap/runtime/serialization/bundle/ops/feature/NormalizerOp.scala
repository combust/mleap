package ml.combust.mleap.runtime.serialization.bundle.ops.feature

import ml.bundle.dsl._
import ml.bundle.op.{OpModel, OpNode}
import ml.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.NormalizerModel
import ml.combust.mleap.runtime.transformer.feature.Normalizer

/**
  * Created by hollinwilkins on 9/24/16.
  */
object NormalizerOp extends OpNode[Normalizer, NormalizerModel] {

  override val Model: OpModel[NormalizerModel] = new OpModel[NormalizerModel] {
    override def opName: String = Bundle.BuiltinOps.feature.normalizer

    override def store(context: BundleContext, model: WritableModel, obj: NormalizerModel): WritableModel = {
      model.withAttr(Attribute("p_norm", Value.double(obj.pNorm)))
    }

    override def load(context: BundleContext, model: ReadableModel): NormalizerModel = {
      NormalizerModel(pNorm = model.value("p_norm").getDouble)
    }
  }

  override def name(node: Normalizer): String = node.uid

  override def model(node: Normalizer): NormalizerModel = node.model

  override def load(context: BundleContext, node: ReadableNode, model: NormalizerModel): Normalizer = {
    Normalizer(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: Normalizer): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
