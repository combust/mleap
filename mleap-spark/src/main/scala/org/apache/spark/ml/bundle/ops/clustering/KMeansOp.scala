package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansOp extends OpNode[SparkBundleContext, KMeansModel, KMeansModel] {
  override val Model: OpModel[SparkBundleContext, KMeansModel] = new OpModel[SparkBundleContext, KMeansModel] {
    override val klazz: Class[KMeansModel] = classOf[KMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(model: Model, obj: KMeansModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("cluster_centers", Value.tensorList(obj.clusterCenters.map(_.toArray).map(Tensor.denseVector)))
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

  override val klazz: Class[KMeansModel] = classOf[KMeansModel]

  override def name(node: KMeansModel): String = node.uid

  override def model(node: KMeansModel): KMeansModel = node

  override def load(node: Node, model: KMeansModel)
                   (implicit context: BundleContext[SparkBundleContext]): KMeansModel = {
    val clusterCenters = model.clusterCenters.map {
      case DenseVector(values) => Vectors.dense(values)
      case SparseVector(size, indices, values) => Vectors.sparse(size, indices, values)
    }

    new KMeansModel(uid = node.name,
      parentModel = new clustering.KMeansModel(clusterCenters)).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: KMeansModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withInput(node.getFeaturesCol, "features", fieldType(node.getFeaturesCol, dataset))
      .withOutput(node.getPredictionCol, "prediction", fieldType(node.getPredictionCol, dataset))
  }
}
