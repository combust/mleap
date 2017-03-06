package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.MleapContext

/**
  * Created by mageswarand on 14/2/17.
  * https://github.com/combust/mleap/blob/master/mleap-runtime/src/main/scala/ml/combust/mleap/bundle/ops/feature/TokenizerOp.scala
  */
class WordLengthFilterOp extends OpNode[MleapContext, WordLengthFilter, WordLengthFilterModel] {

  override val Model: OpModel[MleapContext, WordLengthFilterModel] = new OpModel[MleapContext, WordLengthFilterModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[WordLengthFilterModel] = classOf[WordLengthFilterModel]

    // a unique name for our op
    override def opName: String = "word_filter"

    override def store(model: Model, obj: WordLengthFilterModel)(implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("length", Value.int(obj.length))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): WordLengthFilterModel = {
      new WordLengthFilterModel(model.value("length").getInt)
    }
  }

  // class of the transformer
  override val klazz: Class[WordLengthFilter] = classOf[WordLengthFilter]

  // unique name in the pipeline for this transformer
  override def name(node: WordLengthFilter): String = node.uid

  // the core model that is used by the transformer
  override def model(node: WordLengthFilter): WordLengthFilterModel = node.model


  // the shape defines the inputs and outputs of our node
  // in this case, we have 1 input and 1 output that
  // are connected to the standard input and output ports for
  // a node. shapes can get fairly complicated and may be confusing at first
  // but all they do is connect fields from a data frame to certain input/output
  // locations of the node itself
  override def shape(node: WordLengthFilter): Shape =
  Shape().withStandardIO(node.inputCol, node.outputCol)

  // reconstruct our MLeap transformer from the
  // deserialized core model, unique name of this node,
  // and the inputs/outputs of the node
  override def load(node: Node, model: WordLengthFilterModel)(implicit context: BundleContext[MleapContext]): WordLengthFilter = {

    WordLengthFilter(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }
}
