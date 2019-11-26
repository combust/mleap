package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.Model
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.runtime.transformer.classification.NaiveBayesClassifier
import ml.combust.mleap.core.classification.NaiveBayesModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.linalg.{Matrices, Vectors}


/**
  * Created by fshabbir on 12/25/16.
  */
class NaiveBayesClassifierOp extends MleapOp[NaiveBayesClassifier, NaiveBayesModel]{
  override val Model: OpModel[MleapContext, NaiveBayesModel] = new OpModel[MleapContext, NaiveBayesModel]{
    override val klazz: Class[NaiveBayesModel] = classOf[NaiveBayesModel]

    override def opName: String = Bundle.BuiltinOps.classification.naive_bayes

    override def store(model: Model, obj: NaiveBayesModel)(implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("num_classes", Value.long(obj.numClasses)).
        withValue("pi", Value.vector(obj.pi.toArray)).
        withValue("theta", Value.tensor(DenseTensor(obj.theta.toArray, Seq(obj.theta.numRows, obj.theta.numCols)))).
        withValue("model_type", Value.string(obj.modelType.toString)).
        withValue("thresholds", obj.thresholds.map(Value.doubleList(_)))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): NaiveBayesModel = {
      val theta = model.value("theta").getTensor[Double]
      val modelType = NaiveBayesModel.forName(model.value("model_type").getString)
      val numClasses = model.value("num_classes").getLong.toInt
      val thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray)
      require(thresholds.isEmpty || thresholds.get.length == numClasses,
        "NaiveBayesModel loaded with non-matching numClasses and thresholds.length. " +
          s" numClasses=$numClasses, but thresholds has length ${thresholds.get.length}")
      new NaiveBayesModel(numFeatures = model.value("num_features").getLong.toInt,
        numClasses = numClasses,
        pi = Vectors.dense(model.value("pi").getTensor[Double].toArray),
        theta = Matrices.dense(theta.dimensions.head, theta.dimensions(1), theta.toArray),
        modelType = modelType,
        thresholds = thresholds)
    }

  }
  override def model(node: NaiveBayesClassifier): NaiveBayesModel = node.model
}
