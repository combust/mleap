package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansOp extends SimpleSparkOp[KMeansModel] {
  override val Model: OpModel[SparkBundleContext, KMeansModel] = new OpModel[SparkBundleContext, KMeansModel] {
    override val klazz: Class[KMeansModel] = classOf[KMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(model: Model, obj: KMeansModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("cluster_centers", Value.tensorList(obj.clusterCenters.map(cc => Tensor.denseVector(cc.toArray)))).
        withValue("num_features", Value.long(obj.clusterCenters.head.size))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): KMeansModel = {
      val clusterCenters = model.value("cluster_centers").
        getTensorList[Double].toArray.
        map(t => Vectors.dense(t.toArray))
      val mllibModel = new clustering.KMeansModel(clusterCenters)

      new KMeansModel(uid = "", parentModel = mllibModel)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: KMeansModel): KMeansModel = {
    val clusterCenters = model.clusterCenters.map {
      case DenseVector(values) => Vectors.dense(values)
      case SparseVector(size, indices, values) => Vectors.sparse(size, indices, values)
    }
    new KMeansModel(uid = uid, parentModel = new clustering.KMeansModel(clusterCenters))
  }

  override def sparkInputs(obj: KMeansModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: KMeansModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
}
