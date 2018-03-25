package ml.combust.mleap.bundle.ops.recommendation

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.recommendation.ALSModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.recommendation.ALS
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.linalg.Vectors

class ALSOp extends MleapOp[ALS, ALSModel] {
  override val Model: OpModel[MleapContext, ALSModel] = new OpModel[MleapContext, ALSModel] {
    override val klazz: Class[ALSModel] = classOf[ALSModel]

    override def opName: String = Bundle.BuiltinOps.recommendation.als

    override def store(model: Model, obj: ALSModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (users, userFactors) = obj.userFactors.toSeq.unzip
      val (items, itemFactors) = obj.itemFactors.toSeq.unzip
      model.withValue("rank", Value.int(obj.rank))
           .withValue("users", Value.intList(users))
           .withValue("user_factors", Value.tensorList(userFactors.map(factors => Tensor.denseVector(factors.toArray))))
           .withValue("items", Value.intList(items))
           .withValue("item_factors", Value.tensorList(itemFactors.map(factors => Tensor.denseVector(factors.toArray))))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): ALSModel = {
      val userFactors = model.value("users").getIntList
                        .zip(model.value("user_factors").getTensorList[Float].toArray.map(t => Vectors.dense(t.toArray.map(_.toDouble))))
                        .toMap
      val itemFactors = model.value("items").getIntList
                             .zip(model.value("item_factors").getTensorList[Float].toArray.map(t => Vectors.dense(t.toArray.map(_.toDouble))))
                             .toMap

      ALSModel(rank = model.value("rank").getInt, userFactors = userFactors, itemFactors = itemFactors)
    }
  }

  override def model(node: ALS): ALSModel = node.model
}
