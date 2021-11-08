package org.apache.spark.ml.bundle.ops

import org.apache.spark.ml.PipelineStage

object OpsUtils {

  def copySparkStageParams(fromStage: PipelineStage, toStage: PipelineStage): Unit = {
    /**
      * This method copy params from one spark stage to another spark stage.
      * The "from" stage type must be the same with the "to" stage.
      * Allow "to" stage have different uid with "from" stage".
      *
      * This helper function is a suppliment of spark API `PipelineStage.copy()`
      * which only support copy param between stages with the same uid.
      */
    for (param <- fromStage.params) {
      val paramValueOpt = fromStage.get(param)
      if (paramValueOpt.isDefined) {
        toStage.set(toStage.getParam(param.name), paramValueOpt.get)
      }
    }
  }

}
