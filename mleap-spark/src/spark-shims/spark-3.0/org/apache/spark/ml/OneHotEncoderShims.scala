package org.apache.spark.ml

import org.apache.spark.ml.feature.OneHotEncoder

object OneHotEncoderShims {

  def createOneHotEncoderEstimatorStage(
      inputCols: Array[String],
      outputCols: Array[String]): PipelineStage = {
    new OneHotEncoder()
      .setInputCols(inputCols)
      .setOutputCols(outputCols)
  }

}
