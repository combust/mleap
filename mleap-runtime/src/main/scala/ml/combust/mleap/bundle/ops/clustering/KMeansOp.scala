package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.KMeans
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansOp extends MleapOp[KMeans, KMeansModel] {
  override val Model: OpModel[MleapContext, KMeansModel] = new OpModel[MleapContext, KMeansModel] {
    override val klazz: Class[KMeansModel] = classOf[KMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(model: Model, obj: KMeansModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("cluster_centers",
        Value.tensorList(obj.clusterCenters.map(cc => Tensor.denseVector(cc.vector.toArray))))
      .withValue("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): KMeansModel = {
      val numFeatures = model.value("num_features").getLong.toInt

      KMeansModel(model.value("cluster_centers").getTensorList[Double].map(t => Vectors.dense(t.toArray)), numFeatures)
    }
  }

  override def model(node: KMeans): KMeansModel = node.model
}
