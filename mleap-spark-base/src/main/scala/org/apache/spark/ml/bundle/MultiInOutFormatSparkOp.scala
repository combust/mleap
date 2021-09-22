package org.apache.spark.ml.bundle

import ml.combust.bundle.dsl.{Model, Value}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamValidators
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol, HasOutputCols}

trait MultiInOutFormatSparkOp[
  N <: Transformer with HasInputCol with HasInputCols with HasOutputCol with HasOutputCols
]{

  protected def saveMultiInOutFormat(model: Model, obj: N): Model = {
    ParamValidators.checkSingleVsMultiColumnParams(obj, Seq(obj.inputCol), Seq(obj.inputCols))
    ParamValidators.checkSingleVsMultiColumnParams(obj, Seq(obj.outputCol), Seq(obj.outputCols))
    val result = if(obj.isSet(obj.inputCols)) {
      model.withValue("input_cols", Value.stringList(obj.getInputCols))
    } else {
      model.withValue("input_col", Value.string(obj.getInputCol))
    }
    if (obj.isSet(obj.outputCols)) {
      result.withValue("output_cols", Value.stringList(obj.getOutputCols))
    } else {
      result.withValue("output_col", Value.string(obj.getOutputCol))
    }
  }

  protected def loadMultiInOutFormat(model: Model, obj: N): N = {
    val inputCol = model.getValue("input_col").map(_.getString)
    val inputCols = model.getValue("input_cols").map(_.getStringList)
    val outputCol = model.getValue("output_col").map(_.getString)
    val outputCols = model.getValue("output_cols").map(_.getStringList)
    val result: N = (inputCol, inputCols) match {
      case (None, None) => obj
      case (Some(col), None) => obj.set(obj.inputCol, col)
      case (None, Some(cols)) => obj.set(obj.inputCols, cols.toArray)
      case (_, _) => throw new UnsupportedOperationException("Cannot use both inputCol and inputCols")
    }
    (outputCol, outputCols) match {
      case (None, None) => obj
      case (Some(col), None) => result.set(result.outputCol, col)
      case (None, Some(cols)) => result.set(result.outputCols, cols.toArray)
      case (_, _) => throw new UnsupportedOperationException("Cannot use both outputCol and outputCols")
    }
  }

  def sparkInputs(obj: N): Seq[ParamSpec] = {
    if (obj.isSet(obj.inputCols)) {
      Seq(ParamSpec("input", obj.inputCols))
    } else{
      Seq(ParamSpec("input", obj.inputCol))
    }
  }

  def sparkOutputs(obj: N): Seq[ParamSpec] = {
    if (obj.isSet(obj.outputCols)) {
      Seq(ParamSpec("output", obj.outputCols))
    } else{
      Seq(ParamSpec("output", obj.outputCol))
    }
  }
}
