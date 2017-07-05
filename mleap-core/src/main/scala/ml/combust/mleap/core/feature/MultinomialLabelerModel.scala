package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 1/18/17.
  */
object MultinomialLabelerModel {
  def apply(threshold: Double,
            labels: Seq[String]): MultinomialLabelerModel = {
    MultinomialLabelerModel(threshold, ReverseStringIndexerModel(labels))
  }
}

case class MultinomialLabelerModel(threshold: Double,
                                   indexer: ReverseStringIndexerModel) extends Model {
  def apply(tensor: Tensor[Double]): Seq[(Double, Int, String)] = {
    top(tensor).map(v => (v._1, v._2, indexer(v._2)))
  }

  def top(tensor: Tensor[Double]): Seq[(Double, Int)] = {
    assert(tensor.dimensions.size == 1, "only vectors accepted")

    tensor.toDense.values.
      zipWithIndex.
      filter(_._1 >= threshold)
  }

  def topLabels(tensor: Tensor[Double]): Seq[String] = {
    top(tensor).map(_._2).map(indexer.apply)
  }
}
