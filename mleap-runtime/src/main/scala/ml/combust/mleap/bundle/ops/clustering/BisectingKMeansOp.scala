package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.tree.cluster.NodeSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.clustering.{BisectingKMeansModel, ClusteringTreeNode}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.BisectingKMeans
import ml.combust.mleap.bundle.tree.clustering.MleapNodeWrapper

/**
  * Created by hollinwilkins on 12/26/16.
  */
class BisectingKMeansOp extends MleapOp[BisectingKMeans, BisectingKMeansModel] {
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

  override def model(node: BisectingKMeans): BisectingKMeansModel = node.model
}
