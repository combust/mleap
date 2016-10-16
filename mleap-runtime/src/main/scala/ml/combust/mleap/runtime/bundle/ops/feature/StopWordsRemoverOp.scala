package ml.combust.mleap.runtime.bundle.ops.feature

import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.BundleContext
import ml.combust.mleap.core.feature.StopWordsRemoverModel
import ml.combust.mleap.runtime.transformer.feature.StopWordsRemover

/**
  * Created by mikhail on 10/16/16.
  */
object StopWordsRemoverOp extends OpNode[StopWordsRemover, StopWordsRemoverModel] {
  override val Model: OpModel[StopWordsRemoverModel] = new OpModel[StopWordsRemoverModel] {
    override def opName: String = Bundle.BuiltinOps.feature.stopwords_remover

    override def store(context: BundleContext, model: Model, obj: StopWordsRemoverModel): Model = {
      model.withAttr("stop_words", Value.stringVector(obj.stopWords)).
        withAttr("case_sensitive", Value.boolean(obj.caseSensitive))
    }

    override def load(context: BundleContext, model: Model): StopWordsRemoverModel = {
      StopWordsRemoverModel(stopWords = model.value("stop_words").getStringVector.toArray,
        caseSensitive = model.value("case_sensitive").getBoolean)
    }

  }

  override def name(node: StopWordsRemover): String = node.uid

  override def model(node: StopWordsRemover): StopWordsRemoverModel = node.model

  override def load(context: BundleContext, node: Node, model: StopWordsRemoverModel): StopWordsRemover = {
    StopWordsRemover(inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model
    )
  }

  override def shape(node: StopWordsRemover): Shape = Shape().withStandardIO(node.inputCol, node.outputCol)
}
