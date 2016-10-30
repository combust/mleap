package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 9/30/16.
  */
class KMeansOp extends OpNode[MleapContext, KMeans, KMeansModel] {
  override val Model: OpModel[MleapContext, KMeansModel] = new OpModel[MleapContext, KMeansModel] {
    override val klazz: Class[KMeansModel] = classOf[KMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(model: Model, obj: KMeansModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("cluster_centers",
        Value.tensorList(value = obj.clusterCenters.map(_.vector.toArray.toSeq),
        dims = Seq(-1)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): KMeansModel = {
      KMeansModel(model.value("cluster_centers").getTensorList[Double].map(t => Vectors.dense(t.toArray)))
    }
  }

  override val klazz: Class[KMeans] = classOf[KMeans]

  override def name(node: KMeans): String = node.uid

  override def model(node: KMeans): KMeansModel = node.model

  override def load(node: Node, model: KMeansModel)
                   (implicit context: BundleContext[MleapContext]): KMeans = {
    KMeans(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: KMeans): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
