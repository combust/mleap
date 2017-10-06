package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.BucketedRandomProjectionLSH
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class BucketedRandomProjectionLSHOp extends MleapOp[BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel] {
  override val Model: OpModel[MleapContext, BucketedRandomProjectionLSHModel] = new OpModel[MleapContext, BucketedRandomProjectionLSHModel] {
    override val klazz: Class[BucketedRandomProjectionLSHModel] = classOf[BucketedRandomProjectionLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketed_random_projection_lsh

    override def store(model: Model, obj: BucketedRandomProjectionLSHModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("random_unit_vectors", Value.tensorList[Double](obj.randomUnitVectors.map(v => Tensor.denseVector(v.toArray)))).
        withValue("bucket_length", Value.double(obj.bucketLength)).
        withValue("input_size", Value.int(obj.inputSize))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BucketedRandomProjectionLSHModel = {
      val ruv = model.value("random_unit_vectors").getTensorList[Double].map(_.toArray).map(Vectors.dense)
      BucketedRandomProjectionLSHModel(randomUnitVectors = ruv,
        bucketLength = model.value("bucket_length").getDouble,
        inputSize = model.value("input_size").getInt)
    }
  }

  override def model(node: BucketedRandomProjectionLSH): BucketedRandomProjectionLSHModel = node.model
}
