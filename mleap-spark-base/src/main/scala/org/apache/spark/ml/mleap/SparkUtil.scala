package org.apache.spark.ml.mleap

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 11/18/16.
  */
object SparkUtil {
  def createPipelineModel(stages: Array[Transformer]): PipelineModel = {
    createPipelineModel(uid = Identifiable.randomUID("pipeline"), stages = stages)
  }

  def createPipelineModel(uid: String,
                          stages: Array[Transformer]): PipelineModel = {
    new PipelineModel(uid, stages)
  }
}
