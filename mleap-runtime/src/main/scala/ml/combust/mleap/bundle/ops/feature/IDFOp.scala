package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.feature.IDFModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.IDF
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class IDFOp extends MleapOp[IDF, IDFModel] {
  override val Model: OpModel[MleapContext, IDFModel] = new OpModel[MleapContext, IDFModel] {
    override val klazz: Class[IDFModel] = classOf[IDFModel]

    override def opName: String = Bundle.BuiltinOps.feature.idf

    override def store(model: Model, obj: IDFModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("idf", Value.vector(obj.idf.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): IDFModel = {
      IDFModel(idf = Vectors.dense(model.value("idf").getTensor[Double].toArray))
    }
  }

  override def model(node: IDF): IDFModel = node.model
}
