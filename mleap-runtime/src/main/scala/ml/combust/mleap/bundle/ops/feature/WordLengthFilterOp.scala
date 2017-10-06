package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.WordLengthFilterModel
import ml.combust.mleap.runtime.frame.MleapContext
import ml.combust.mleap.runtime.transformer.feature.WordLengthFilter

/**
  * Created by mageswarand on 14/2/17.
  */
class WordLengthFilterOp extends MleapOp[WordLengthFilter, WordLengthFilterModel] {

  override val Model: OpModel[MleapContext, WordLengthFilterModel] = new OpModel[MleapContext, WordLengthFilterModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[WordLengthFilterModel] = classOf[WordLengthFilterModel]

    // a unique name for our op
    override def opName: String = Bundle.BuiltinOps.feature.word_filter

    override def store(model: Model, obj: WordLengthFilterModel)(implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("length", Value.int(obj.length))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): WordLengthFilterModel = {
      new WordLengthFilterModel(model.value("length").getInt)
    }
  }

  // the core model that is used by the transformer
  override def model(node: WordLengthFilter): WordLengthFilterModel = node.model
}
