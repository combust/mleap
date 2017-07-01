package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.tree.cluster.NodeSerializer
import ml.combust.mleap.core.clustering.{BisectingKMeansModel, ClusteringTreeNode}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.BisectingKMeans
import ml.combust.mleap.bundle.tree.clustering.MleapNodeWrapper

/**
  * Created by hollinwilkins on 12/26/16.
  */
class BisectingKMeansOp extends OpNode[MleapContext, BisectingKMeans, BisectingKMeansModel] {
  implicit val nodeWrapper = MleapNodeWrapper

  override val Model: OpModel[MleapContext, BisectingKMeansModel] = new OpModel[MleapContext, BisectingKMeansModel] {
    override val klazz: Class[BisectingKMeansModel] = classOf[BisectingKMeansModel]

    override def opName: String = Bundle.BuiltinOps.clustering.bisecting_k_means

    override def store(model: Model, obj: BisectingKMeansModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      NodeSerializer[ClusteringTreeNode](context.file("tree")).write(obj.root)
      model
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BisectingKMeansModel = {
      val root = NodeSerializer[ClusteringTreeNode](context.file("tree")).read().get
      BisectingKMeansModel(root)
    }
  }

  override val klazz: Class[BisectingKMeans] = classOf[BisectingKMeans]

  override def name(node: BisectingKMeans): String = node.uid

  override def model(node: BisectingKMeans): BisectingKMeansModel = node.model

  override def load(node: Node, model: BisectingKMeansModel)
                   (implicit context: BundleContext[MleapContext]): BisectingKMeans = {
    BisectingKMeans(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: BisectingKMeans): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
