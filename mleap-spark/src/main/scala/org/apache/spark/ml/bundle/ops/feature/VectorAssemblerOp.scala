package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.bundle.{BundleHelper, SparkBundleContext}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.mleap.TypeConverters.mleapType
import ml.combust.mleap.runtime.types.BundleTypeConverters.mleapTypeToBundleType

/**
  * Created by hollinwilkins on 8/21/16.
  */
class VectorAssemblerOp extends OpNode[SparkBundleContext, VectorAssembler, VectorAssembler] {
  override val Model: OpModel[SparkBundleContext, VectorAssembler] = new OpModel[SparkBundleContext, VectorAssembler] {
    override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

    override def opName: String = Bundle.BuiltinOps.feature.vector_assembler

    override def store(model: Model, obj: VectorAssembler)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      assert(context.context.dataset.isDefined, BundleHelper.sampleDataframeMessage(klazz))

      val dataset = context.context.dataset.get
      val inputTypes = obj.getInputCols.map(i => mleapTypeToBundleType(mleapType(dataset.schema(i).dataType)))

      model.withAttr("input_types", Value.dataTypeList(inputTypes))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): VectorAssembler = { new VectorAssembler(uid = "") }
  }

  override val klazz: Class[VectorAssembler] = classOf[VectorAssembler]

  override def name(node: VectorAssembler): String = node.uid

  override def model(node: VectorAssembler): VectorAssembler = node

  override def load(node: Node, model: VectorAssembler)
                   (implicit context: BundleContext[SparkBundleContext]): VectorAssembler = {
    new VectorAssembler(uid = node.name).
      setInputCols(node.shape.inputs.map(_.name).toArray).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorAssembler): NodeShape = {
    var i = 0
    node.getInputCols.foldLeft(NodeShape()) {
      case (shape, inputCol) =>
        val shape2 = shape.withInput(inputCol, s"input$i")
        i += 1
        shape2
    }.withStandardOutput(node.getOutputCol)
  }
}
