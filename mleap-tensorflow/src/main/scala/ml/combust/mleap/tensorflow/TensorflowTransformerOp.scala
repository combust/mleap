package ml.combust.mleap.tensorflow

import java.nio.file.Files

import ml.bundle.DataType.DataType
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core
import ml.combust.mleap.runtime.{MleapContext, types}
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by hollinwilkins on 1/15/17.
  */
class TensorflowTransformerOp extends OpNode[MleapContext, TensorflowTransformer, TensorflowModel] {
  override val Model: OpModel[MleapContext, TensorflowModel] = new OpModel[MleapContext, TensorflowModel] {
    override val klazz: Class[TensorflowModel] = classOf[TensorflowModel]

    override def opName: String = Bundle.BuiltinOps.tensorflow

    override def store(model: Model, obj: TensorflowModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      Files.write(context.file("graph.pb"), obj.graph.toGraphDef)
      val (inputNames, inputMleapDataTypes) = obj.inputs.unzip
      val (outputNames, outputMleapDataTypes) = obj.outputs.unzip

      val inputDataTypes = inputMleapDataTypes.map(v => v: DataType)
      val outputDataTypes = outputMleapDataTypes.map(v => v: DataType)

      model.withAttr("input_names", Value.stringList(inputNames)).
        withAttr("input_types", Value.dataTypeList(inputDataTypes)).
        withAttr("output_names", Value.stringList(outputNames)).
        withAttr("output_types", Value.dataTypeList(outputDataTypes)).
        withAttr("nodes", obj.nodes.map(Value.stringList))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): TensorflowModel = {
      val graphBytes = Files.readAllBytes(context.file("graph.pb"))
      val inputNames = model.value("input_names").getStringList
      val inputTypes = model.value("input_types").getDataTypeList.map(v => v: core.types.DataType)
      val outputNames = model.value("output_names").getStringList
      val outputTypes = model.value("output_types").getDataTypeList.map(v => v: core.types.DataType)
      val nodes = model.getValue("nodes").map(_.getStringList)

      val inputs = inputNames.zip(inputTypes)
      val outputs = outputNames.zip(outputTypes)

      val graph = new org.tensorflow.Graph()
      graph.importGraphDef(graphBytes)
      TensorflowModel(graph,
        inputs,
        outputs,
        nodes)
    }
  }

  override val klazz: Class[TensorflowTransformer] = classOf[TensorflowTransformer]

  override def name(node: TensorflowTransformer): String = node.uid

  override def model(node: TensorflowTransformer): TensorflowModel = node.model

  override def load(node: Node, model: TensorflowModel)
                   (implicit context: BundleContext[MleapContext]): TensorflowTransformer = {
    TensorflowTransformer(uid = node.name,
      inputs = node.shape.inputs,
      outputs = node.shape.outputs,
      model = model)
  }

  override def shape(node: TensorflowTransformer): NodeShape = {
    NodeShape(node.inputs, node.outputs)
  }
}
