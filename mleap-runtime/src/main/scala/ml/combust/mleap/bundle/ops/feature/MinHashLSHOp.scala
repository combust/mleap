package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.MinHashLSHModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.MinHashLSH

/**
  * Created by hollinwilkins on 12/28/16.
  */
class MinHashLSHOp extends MleapOp[MinHashLSH, MinHashLSHModel] {
  override val Model: OpModel[MleapContext, MinHashLSHModel] = new OpModel[MleapContext, MinHashLSHModel] {
    override val klazz: Class[MinHashLSHModel] = classOf[MinHashLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_hash_lsh

    override def store(model: Model, obj: MinHashLSHModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (ca, cb) = obj.randomCoefficients.unzip

      model.withValue("random_coefficients_a", Value.longList(ca.map(_.toLong))).
        withValue("random_coefficients_b", Value.longList(cb.map(_.toLong)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MinHashLSHModel = {
      val ca = model.value("random_coefficients_a").getLongList.map(_.toInt)
      val cb = model.value("random_coefficients_b").getLongList.map(_.toInt)
      val randomCoefficients = ca.zip(cb)
      MinHashLSHModel(randomCoefficients = randomCoefficients)
    }
  }

  override def model(node: MinHashLSH): MinHashLSHModel = node.model
}
