package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansOp extends OpNode[SparkBundleContext, KMeansModel, KMeansModel] {
  override val Model: OpModel[SparkBundleContext, KMeansModel] = new OpModel[SparkBundleContext, KMeansModel] {
    override val klazz: Class[KMeansModel] = classOf[KMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: KMeansModel): Model = {
      model.withAttr("cluster_centers", Value.tensorList(
        value = obj.clusterCenters.map(_.toArray.toSeq),
        dims = Seq(-1)))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): KMeansModel = {
      val clusterCenters = model.value("cluster_centers").
        getTensorList[Double].toArray.
        map(t => Vectors.dense(t.toArray))
      val mllibModel = new clustering.KMeansModel(clusterCenters)

      new KMeansModel(uid = "", parentModel = mllibModel)
    }
  }

  override val klazz: Class[KMeansModel] = classOf[KMeansModel]

  override def name(node: KMeansModel): String = node.uid

  override def model(node: KMeansModel): KMeansModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: KMeansModel): KMeansModel = {
    val clusterCenters = model.clusterCenters.map {
      case DenseVector(values) => Vectors.dense(values)
      case SparseVector(size, indices, values) => Vectors.sparse(size, indices, values)
    }

    new KMeansModel(uid = node.name,
      parentModel = new clustering.KMeansModel(clusterCenters)).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: KMeansModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}
