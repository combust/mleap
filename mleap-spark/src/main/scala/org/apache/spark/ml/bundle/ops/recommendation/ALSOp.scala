package org.apache.spark.ml.bundle.ops.recommendation

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession

class ALSOp extends SimpleSparkOp[ALSModel] {
  override val Model: OpModel[SparkBundleContext, ALSModel] = new OpModel[SparkBundleContext, ALSModel] {
    override val klazz: Class[ALSModel] = classOf[ALSModel]

    override def opName: String = Bundle.BuiltinOps.recommendation.als

    override def store(model: Model, obj: ALSModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val sparkSession = SparkSession.builder().getOrCreate()
      import sparkSession.implicits._

      val (users, userFactors) = obj.userFactors.select("id", "features")
                                                .map(row => (row.getInt(0), row.getSeq[Float](1))).collect().unzip
      val (items, itemFactors) = obj.itemFactors.select("id", "features")
                                                .map(row => (row.getInt(0), row.getSeq[Float](1))).collect().unzip
      model.withValue("rank", Value.int(obj.rank))
           .withValue("users", Value.intList(users))
           .withValue("user_factors", Value.tensorList(userFactors.map(factors => Tensor.denseVector(factors.toArray))))
           .withValue("items", Value.intList(items))
           .withValue("item_factors", Value.tensorList(itemFactors.map(factors => Tensor.denseVector(factors.toArray))))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ALSModel = {
      val sparkSession = SparkSession.builder().getOrCreate()

      val userFactors = sparkSession.createDataFrame(model.value("users").getIntList
        .zip(model.value("user_factors").getTensorList[Float].map(t => t.toArray)))
      val itemFactors = sparkSession.createDataFrame(model.value("items").getIntList
        .zip(model.value("item_factors").getTensorList[Float].map(t => t.toArray)))

      new ALSModel(uid = "", userFactors = userFactors, itemFactors = itemFactors, rank = model.value("rank").getInt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: ALSModel): ALSModel = {
    new ALSModel(uid = uid, rank = model.rank, userFactors = model.userFactors, itemFactors = model.itemFactors)
  }

  override def sparkInputs(obj: ALSModel): Seq[ParamSpec] = {
    Seq("user" -> obj.userCol, "item" ->  obj.itemCol)
  }

  override def sparkOutputs(obj: ALSModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.predictionCol)
  }
}


