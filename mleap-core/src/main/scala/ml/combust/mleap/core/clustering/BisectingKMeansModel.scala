package ml.combust.mleap.core.clustering

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.linalg.LinalgUtils
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.mleap.VectorWithNorm

import scala.annotation.tailrec

/**
  * Created by hollinwilkins on 12/26/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/clustering/BisectingKMeansModel.scala")
case class BisectingKMeansModel(root: ClusteringTreeNode) extends Model {
  lazy val numFeatures: Int = root.centerWithNorm.vector.size
  lazy val clusterCenters: Array[Vector] = root.leafNodes.map(_.center)
  lazy val k: Int = clusterCenters.length

  def apply(features: Vector): Int = {
    root.predict(features)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("prediction" -> ScalarType.Int.nonNullable).get
}

@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/mllib/clustering/BisectingKMeansModel.scala")
case class ClusteringTreeNode(index: Int,
                              centerWithNorm: VectorWithNorm,
                              children: Array[ClusteringTreeNode]) extends Serializable {

  /** Whether this is a leaf node. */
  val isLeaf: Boolean = children.isEmpty

  def numChildren: Int = children.length

  require((isLeaf && index >= 0) || (!isLeaf && index < 0))

  /** Cluster center. */
  def center: Vector = centerWithNorm.vector

  /** Predicts the leaf cluster node index that the input point belongs to. */
  def predict(point: Vector): Int = {
    val (index, _) = predict(VectorWithNorm(point))
    index
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    */
  private def predict(pointWithNorm: VectorWithNorm): (Int, Double) = {
    predict(pointWithNorm, LinalgUtils.fastSquaredDistance(centerWithNorm, pointWithNorm))
  }

  /**
    * Predicts the cluster index and the cost of the input point.
    *
    * @param pointWithNorm input point
    * @param cost the cost to the current center
    * @return (predicted leaf cluster index, cost)
    */
  @tailrec
  private def predict(pointWithNorm: VectorWithNorm, cost: Double): (Int, Double) = {
    if (isLeaf) {
      (index, cost)
    } else {
      val (selectedChild, minCost) = children.map { child =>
        (child, LinalgUtils.fastSquaredDistance(child.centerWithNorm, pointWithNorm))
      }.minBy(_._2)
      selectedChild.predict(pointWithNorm, minCost)
    }
  }

  /**
    * Returns all leaf nodes from this node.
    */
  def leafNodes: Array[ClusteringTreeNode] = {
    if (isLeaf) {
      Array(this)
    } else {
      children.flatMap(_.leafNodes)
    }
  }
}
