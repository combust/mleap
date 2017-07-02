package ml.combust.mleap.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.IDFModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.feature.IDF
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class IDFOp extends OpNode[MleapContext, IDF, IDFModel] {
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

  override val klazz: Class[IDF] = classOf[IDF]

  override def name(node: IDF): String = node.uid

  override def model(node: IDF): IDFModel = node.model

  override def load(node: Node, model: IDFModel)
                   (implicit context: BundleContext[MleapContext]): IDF = {
    IDF(uid = node.name,
      inputCol = node.shape.standardInput.name,
      outputCol = node.shape.standardOutput.name,
      model = model)
  }

  override def shape(node: IDF): NodeShape = NodeShape().withStandardIO(node.inputCol, node.outputCol)
}
