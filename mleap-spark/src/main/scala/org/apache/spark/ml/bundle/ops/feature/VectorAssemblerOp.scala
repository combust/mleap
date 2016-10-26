package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.VectorAssembler

/**
  * Created by hollinwilkins on 8/21/16.
  */
class VectorAssemblerOp extends OpNode[SparkBundleContext, VectorAssembler, VectorAssembler] {
  override val Model: OpModel[SparkBundleContext, VectorAssembler] = new OpModel[SparkBundleContext, VectorAssembler] {
    override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: VectorAssembler): Model = { model }

    override def load(context: BundleContext[SparkBundleContext], model: Model): VectorAssembler = { new VectorAssembler(uid = "") }
  }

  override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssembler = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: VectorAssembler): VectorAssembler = {
    new VectorAssembler().
      setInputCols(node.shape.inputs.map(_.name).toArray).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorAssembler): Shape = {
    var i = 0
    node.getInputCols.foldLeft(Shape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.getOutputCol)
  }
}
