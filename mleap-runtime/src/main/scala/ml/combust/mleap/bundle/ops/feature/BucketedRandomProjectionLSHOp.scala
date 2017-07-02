package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.BucketedRandomProjectionLSHModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.BucketedRandomProjectionLSH
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class BucketedRandomProjectionLSHOp extends OpNode[MleapContext, BucketedRandomProjectionLSH, BucketedRandomProjectionLSHModel] {
  override val Model: OpModel[MleapContext, BucketedRandomProjectionLSHModel] = new OpModel[MleapContext, BucketedRandomProjectionLSHModel] {
    override val klazz: Class[BucketedRandomProjectionLSHModel] = classOf[BucketedRandomProjectionLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketed_random_projection_lsh

    override def store(model: Model, obj: BucketedRandomProjectionLSHModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("random_unit_vectors", Value.tensorList[Double](obj.randomUnitVectors.map(v => Tensor.denseVector(v.toArray)))).
        withValue("bucket_length", Value.double(obj.bucketLength))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BucketedRandomProjectionLSHModel = {
      val ruv = model.value("random_unit_vectors").getTensorList[Double].map(_.toArray).map(Vectors.dense)
      BucketedRandomProjectionLSHModel(randomUnitVectors = ruv,
        bucketLength = model.value("bucket_length").getDouble)
    }
  }

  override val klazz: Class[BucketedRandomProjectionLSH] = classOf[BucketedRandomProjectionLSH]

  override def name(node: BucketedRandomProjectionLSH): String = node.uid

  override def model(node: BucketedRandomProjectionLSH): BucketedRandomProjectionLSHModel = node.model

  override def load(node: Node, model: BucketedRandomProjectionLSHModel)
                   (implicit context: BundleContext[MleapContext]): BucketedRandomProjectionLSH = {
    BucketedRandomProjectionLSH(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: BucketedRandomProjectionLSH): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
