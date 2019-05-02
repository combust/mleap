package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.core.types.TensorShape
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle._
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.mleap.TypeConverters.sparkToMleapDataShape

/**
  * Created by hollinwilkins on 12/28/16.
  */
class BucketedRandomProjectionLSHOp extends SimpleSparkOp[BucketedRandomProjectionLSHModel] {
  override val Model: OpModel[SparkBundleContext, BucketedRandomProjectionLSHModel] = new OpModel[SparkBundleContext, BucketedRandomProjectionLSHModel] {
    override val klazz: Class[BucketedRandomProjectionLSHModel] = classOf[BucketedRandomProjectionLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.bucketed_random_projection_lsh

    override def store(model: Model, obj: BucketedRandomProjectionLSHModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val dataset = context.context.dataset.get
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getInputCol), dataset).asInstanceOf[TensorShape]

      model.withValue("random_unit_vectors", Value.tensorList[Double](obj.randUnitVectors.map(_.toArray).map(Tensor.denseVector))).
        withValue("bucket_length", Value.double(obj.getBucketLength))
        .withValue("input_size", Value.int(inputShape.dimensions.get(0)))

    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): BucketedRandomProjectionLSHModel = {
      val ruv = model.value("random_unit_vectors").getTensorList[Double].map(_.toArray).map(Vectors.dense)
      val m = new BucketedRandomProjectionLSHModel(uid = "", randUnitVectors = ruv.toArray)
      m.set(m.bucketLength, model.value("bucket_length").getDouble)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: BucketedRandomProjectionLSHModel): BucketedRandomProjectionLSHModel = {
    val m = new BucketedRandomProjectionLSHModel(uid = uid, randUnitVectors = model.randUnitVectors)
    m.set(m.bucketLength, model.getBucketLength)
  }

  override def sparkInputs(obj: BucketedRandomProjectionLSHModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: BucketedRandomProjectionLSHModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }

}

