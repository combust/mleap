package ml.combust.mleap.runtime.transformer.clustering

import ml.combust.mleap.core.clustering.LocalLDAModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by mageswarand on 3/3/17.
  */
case class LDAModel(override val uid: String = Transformer.uniqueName("lda"),
                    featureCol: String,
                    topicDistributionCol: String,
                    model: LocalLDAModel) extends Transformer {

  println("Modle Paramaters @ LDAModelCT...")
  println("Rows: " + model.topicsMatrix.rows)
  println("Cols: " + model.topicsMatrix.cols)
  println("Array: " + model.topicsMatrix.values.size)
  println(featureCol)
  println(topicDistributionCol)

  val topicDistribution: UserDefinedFunction = (features: BDV[Double]) => model.topicDistribution(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(topicDistributionCol, featureCol)(topicDistribution)
  }
}
