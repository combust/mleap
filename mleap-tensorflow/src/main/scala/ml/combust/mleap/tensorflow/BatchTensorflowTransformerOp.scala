package ml.combust.mleap.tensorflow

import java.nio.file.Files

import ml.bundle.{BasicType, DataShape}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core
import ml.combust.mleap.core.types.TensorType
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.types.BundleTypeConverters._

class BatchTensorflowTransformerOp extends MleapOp[BatchTensorflowTransformer, BatchTensorflowModel] {
  override val Model: OpModel[MleapContext, BatchTensorflowModel] = new OpModel[MleapContext, BatchTensorflowModel] {
    override val klazz: Class[BatchTensorflowModel] = classOf[BatchTensorflowModel]

    override def opName: String = Bundle.BuiltinOps.batchTensorflow

    override def store(model: Model, obj: BatchTensorflowModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      Files.write(context.file("graph.pb"), obj.graph.toGraphDef)
      val (inputNames, inputMleapDataTypes) = obj.inputs.unzip
      val (inputBasicTypes, inputShapes) = inputMleapDataTypes.map {
        dt => (dt.base: BasicType, dt.shape: DataShape)
      }.unzip

      val (outputNames, outputMleapDataTypes) = obj.outputs.unzip
      val (outputBasicTypes, outputShapes) = outputMleapDataTypes.map {
        dt => (dt.base: BasicType, dt.shape: DataShape)
      }.unzip

      model.withValue("input_names", Value.stringList(inputNames)).
        withValue("input_types", Value.basicTypeList(inputBasicTypes)).
        withValue("input_shapes", Value.dataShapeList(inputShapes)).
        withValue("output_names", Value.stringList(outputNames)).
        withValue("output_types", Value.basicTypeList(outputBasicTypes)).
        withValue("output_shapes", Value.dataShapeList(outputShapes)).
        withValue("nodes", obj.nodes.map(Value.stringList))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): BatchTensorflowModel = {
      val graphBytes = Files.readAllBytes(context.file("graph.pb"))

      val inputNames = model.value("input_names").getStringList
      val inputTypes = model.value("input_types").getBasicTypeList.map(v => v: core.types.BasicType)
      val inputShapes = model.value("input_shapes").getDataShapeList.map(v => v: core.types.DataShape)

      val outputNames = model.value("output_names").getStringList
      val outputTypes = model.value("output_types").getBasicTypeList.map(v => v: core.types.BasicType)
      val outputShapes = model.value("output_shapes").getDataShapeList.map(v => v: core.types.DataShape)

      val nodes = model.getValue("nodes").map(_.getStringList)

      val inputs = inputNames.zip(inputTypes.zip(inputShapes).map {
        case (b, s) => core.types.DataType(b, s).asInstanceOf[TensorType]
      })
      val outputs = outputNames.zip(outputTypes.zip(outputShapes).map {
        case (b, s) => core.types.DataType(b, s).asInstanceOf[TensorType]
      })

      val graph = new org.tensorflow.Graph()
      graph.importGraphDef(graphBytes)
      BatchTensorflowModel(graph,
        inputs,
        outputs,
        nodes)
    }
  }

  override def model(node: BatchTensorflowTransformer): BatchTensorflowModel = node.model
}