package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MultiInOutMleapOp
import ml.combust.mleap.core.feature.{HandleInvalid, StringIndexerModel}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.StringIndexer

/**
  * Created by hollinwilkins on 8/22/16.
  */
class StringIndexerOp extends MultiInOutMleapOp[StringIndexer, StringIndexerModel] {
  override val Model: OpModel[MleapContext, StringIndexerModel] = new OpModel[MleapContext, StringIndexerModel] {
    override val klazz: Class[StringIndexerModel] = classOf[StringIndexerModel]

    override def opName: String = Bundle.BuiltinOps.feature.string_indexer

    override def store(model: Model, obj: StringIndexerModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
        val m = model.
          withValue("labels_length", Value.int(1)).
          withValue("handle_invalid", Value.string(obj.handleInvalid.asParamString))
        obj.labelsArray.zipWithIndex.foldLeft(m){
          case (m, (labels, i)) => m.withValue(s"labels_array_$i",  Value.stringList(labels))
        }
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): StringIndexerModel = {
      val handleInvalid = model.getValue("handle_invalid").map(_.getString).map(HandleInvalid.fromString(_)).getOrElse(HandleInvalid.default)
      val label_length = model.getValue("labels_length").map(_.getInt).getOrElse(-1)
      val labelsArray: Seq[Seq[String]] = label_length match {
        case -1 =>
          // backawards compatibility with spark v2
          Seq(model.value("labels").getStringList)
        case _ =>  (0 until label_length).map(i=>model.value(s"labels_array_$i").getStringList)
      }
      StringIndexerModel(labelsArray = labelsArray, handleInvalid = handleInvalid)
    }
  }

  override def model(node: StringIndexer): StringIndexerModel = node.model

}
