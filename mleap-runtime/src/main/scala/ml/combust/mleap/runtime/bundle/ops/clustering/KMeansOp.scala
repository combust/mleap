package ml.combust.mleap.runtime.bundle.ops.clustering

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.clustering.KMeansModel
import ml.combust.mleap.runtime.transformer.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 9/30/16.
  */
object KMeansOp extends OpNode[KMeans, KMeansModel] {
  override val Model: OpModel[KMeansModel] = new OpModel[KMeansModel] {
    override def opName: String = Bundle.BuiltinOps.clustering.k_means

    override def store(context: BundleContext, model: Model, obj: KMeansModel): Model = {
      model.withAttr(Attribute("cluster_centers",
        Value.tensorList(value = obj.clusterCenters.map(_.vector.toArray.toSeq),
        dims = Seq(-1))))
    }

    override def load(context: BundleContext, model: Model): KMeansModel = {
      KMeansModel(model.value("cluster_centers").getTensorList[Double].map(t => Vectors.dense(t.toArray)))
    }
  }

  override def name(node: KMeans): String = node.uid

  override def model(node: KMeans): KMeansModel = node.model

  override def load(context: BundleContext, node: Node, model: KMeansModel): KMeans = {
    KMeans(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: KMeans): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
