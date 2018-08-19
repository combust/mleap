package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.RegexIndexerModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.RegexIndexer

class RegexIndexerOp extends MleapOp[RegexIndexer, RegexIndexerModel] {
  override val Model: OpModel[MleapContext, RegexIndexerModel] = new OpModel[MleapContext, RegexIndexerModel] {
    override val klazz: Class[RegexIndexerModel] = classOf[RegexIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.regex_indexer

    override def store(model: Model, obj: RegexIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (rs, is) = obj.lookup.unzip

      val regex = rs.map(_.pattern.toString)
      val indices = is

      model.withValue("regexes", Value.stringList(regex)).
        withValue("indices", Value.intList(indices)).
        withValue("default_value", obj.defaultIndex.map(Value.int))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): RegexIndexerModel = {
      val regex = model.value("regexes").getStringList.map {
        r => r.r.unanchored
      }
      val indices = model.value("indices").getIntList
      val lookup = regex.zip(indices)

      RegexIndexerModel(lookup, model.getValue("default_value").map(_.getInt))
    }
  }

  override def model(node: RegexIndexer): RegexIndexerModel = node.model
}
