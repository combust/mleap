package org.apache.spark.mllib.clustering.bundle.tree.clustering

import java.nio.file.Path

import ml.combust.bundle.BundleContext
import ml.combust.bundle.tree.cluster.NodeSerializer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, ClusteringTreeNode}

/**
  * Created by hollinwilkins on 12/27/16.
  */
object ClusteringTreeNodeUtil {
  implicit val nodeWrapper = SparkNodeWrapper

  def write(bisectingKMeansModel: BisectingKMeansModel)
           (implicit context: BundleContext[SparkBundleContext]): Unit = {
    NodeSerializer[ClusteringTreeNode](context.file("tree")).write(bisectingKMeansModel.root)
  }

  def read()
          (implicit context: BundleContext[SparkBundleContext]): BisectingKMeansModel = {
    new BisectingKMeansModel(NodeSerializer[ClusteringTreeNode](context.file("tree")).read().get)
  }
}
