package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.{Binarizer, BucketedRandomProjectionLSHModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.Param

/**
  * Created by hollinwilkins on 12/28/16.
  */
class BucketedRandomProjectionLSHOp extends SimpleSparkOp[BucketedRandomProjectionLSHModel] {
  override val Model: OpModel[SparkBundleContext, BucketedRandomProjectionLSHModel] = new OpModel[SparkBundleContext, BucketedRandomProjectionLSHModel] {
    override val klazz: Class[BucketedRandomProjectionLSHModel] = classOf[BucketedRandomProjectionLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketed_random_projection_lsh

    override def store(model: Model, obj: BucketedRandomProjectionLSHModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("random_unit_vectors", Value.tensorList[Double](obj.randUnitVectors.map(_.toArray).map(Tensor.denseVector))).
        withValue("bucket_length", Value.double(obj.getBucketLength))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): BucketedRandomProjectionLSHModel = {
      val ruv = model.value("random_unit_vectors").getTensorList[Double].map(_.toArray).map(Vectors.dense)
      val m = new BucketedRandomProjectionLSHModel(uid = "",
        randUnitVectors = ruv.toArray)
      m.set(m.bucketLength, model.value("bucket_length").getDouble)

      m
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: BucketedRandomProjectionLSHModel): BucketedRandomProjectionLSHModel = {
    new BucketedRandomProjectionLSHModel(uid = uid,
      randUnitVectors = model.randUnitVectors)
  }

  override def sparkInputs(obj: BucketedRandomProjectionLSHModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: BucketedRandomProjectionLSHModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}

