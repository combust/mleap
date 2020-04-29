package org.apache.spark.ml

import org.apache.spark.ml.feature.OneHotEncoderEstimator

object OneHotEncoderShims {

  def createOneHotEncoderEstimatorStage(
      inputCols: Array[String],
      outputCols: Array[String]): PipelineStage = {
    new OneHotEncoderEstimator()
      .setInputCols(inputCols)
      .setOutputCols(outputCols)
  }

}
