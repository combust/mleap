package ml.combust.mleap.xgboost.runtime.bundle.ops

import java.nio.file.Files

import com.yelp.xgboost.parser.PredictorFactory
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.xgboost.runtime._


/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostPredictorClassificationOp extends MleapOp[XGBoostPredictorClassification, XGBoostPredictorClassificationModel] {

  override val Model: OpModel[MleapContext, XGBoostPredictorClassificationModel] = new OpModel[MleapContext, XGBoostPredictorClassificationModel] {
  override val klazz: Class[XGBoostPredictorClassificationModel] = classOf[XGBoostPredictorClassificationModel]

  override def opName: String = "xgboost.classifier"

  override def store(model: Model, obj: XGBoostPredictorClassificationModel)
                    (implicit context: BundleContext[MleapContext]): Model = {
    val rawModel = Option(obj.predictor.getRawModel).getOrElse(
      throw new RuntimeException(
        "Cannot store an XGBoostPredictor model that was not loaded from serialized bytes."))
    Files.write(context.file("xgboost.model"), rawModel)
    model
      .withValue("num_classes", Value.int(obj.numClasses))
      .withValue("num_features", Value.int(obj.numFeatures))
      .withValue("tree_limit", Value.int(obj.treeLimit))
      .withValue("treats_zero_as_missing", Value.boolean(obj.treatsZeroAsNA))
  }

  override def load(model: Model)
                   (implicit context: BundleContext[MleapContext]): XGBoostPredictorClassificationModel = {

    val predictor = PredictorFactory.fromModelStream(Files.newInputStream(context.file("xgboost.model")))

    val numClasses = model.value("num_classes").getInt
    val numFeatures = model.value("num_features").getInt
    val treeLimit = model.getValue("tree_limit")
      .map(_.getInt).getOrElse(0)
    val treatsZeroAsNA = treatsZeroAsMissing(model)

    val impl = if(numClasses == 2) {
      XGBoostPredictorBinaryClassificationModel(predictor, numFeatures, treeLimit, treatsZeroAsNA)
    } else {
      XGBoostPredictorMultinomialClassificationModel(predictor, numClasses, numFeatures, treeLimit, treatsZeroAsNA)
    }

    XGBoostPredictorClassificationModel(impl)
  }
}

  override def model(node: XGBoostPredictorClassification): XGBoostPredictorClassificationModel = node.model

  /**
   * A model trained with missing=0.0f treats 0.0 features as absent. Bundles written by the Spark
   * ops carry the raw `missing` float; bundles re-serialized by this op carry the derived
   * `treats_zero_as_missing` boolean. Support both, defaulting to false (missing=NaN semantics).
   */
  private def treatsZeroAsMissing(model: Model): Boolean =
    model.getValue("treats_zero_as_missing").map(_.getBoolean)
      .orElse(model.getValue("missing").map(_.getFloat == 0.0f))
      .getOrElse(false)
}
