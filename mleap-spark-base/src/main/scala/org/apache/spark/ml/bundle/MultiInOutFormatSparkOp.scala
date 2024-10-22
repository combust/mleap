package org.apache.spark.ml.bundle

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamValidators
import org.apache.spark.ml.param.shared._
import ml.combust.bundle.op.OpModel

import scala.reflect.ClassTag

abstract class MultiInOutOpModel[N <: Transformer with HasInputCol with HasInputCols with HasOutputCol with HasOutputCols] extends  OpModel[SparkBundleContext, N] {
  private def validateParams(obj: N): Unit = {
    ParamValidators.checkSingleVsMultiColumnParams(obj, Seq(obj.inputCol), Seq(obj.inputCols))
    ParamValidators.checkSingleVsMultiColumnParams(obj, Seq(obj.outputCol), Seq(obj.outputCols))
  }
  override def store(model: Model, obj: N)(implicit context: BundleContext[SparkBundleContext]): Model = {
    validateParams(obj)
    model
  }
}

abstract class MultiInOutFormatSparkOp[
  N <: Transformer with HasInputCol with HasInputCols with HasOutputCol with HasOutputCols
](implicit ct: ClassTag[N]) extends  SimpleSparkOp[N] {
  import NodeShape._

  override def load(node: Node, model: N)(implicit context: BundleContext[SparkBundleContext]): N = {
    val n = sparkLoad(node.name, node.shape, model)
    SparkShapeLoader(node.shape, n, sparkInputs(n, node.shape), sparkOutputs(n, node.shape)).loadShape()
    n
  }

  def sparkInputs(obj: N, shape: NodeShape): Seq[ParamSpec] = sparkInputs(shape.getInput(standardInputPort).isDefined, obj)

  def sparkOutputs(obj: N, shape: NodeShape): Seq[ParamSpec] = sparkOutputs(shape.getOutput(standardOutputPort).isDefined, obj)

  def sparkInputs(obj: N): Seq[ParamSpec] = sparkInputs(obj.isSet(obj.inputCol), obj)

  def sparkOutputs(obj: N): Seq[ParamSpec] = sparkOutputs(obj.isSet(obj.outputCol), obj)

  def sparkInputs(hasInputCol: Boolean, obj: N): Seq[ParamSpec] = if (hasInputCol) {
    Seq(ParamSpec(standardInputPort, obj.inputCol))
  } else {
    Seq(ParamSpec(standardInputPort, obj.inputCols))
  }

  def sparkOutputs(hasOutputCol: Boolean, obj: N): Seq[ParamSpec] = if (hasOutputCol) {
    Seq(ParamSpec(standardOutputPort, obj.outputCol))
  } else {
    Seq(ParamSpec(standardOutputPort, obj.outputCols))
  }
}