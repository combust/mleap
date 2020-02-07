package ml.combust.mleap.xgboost.runtime

import java.io.File

import ml.combust.mleap.core.types._

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.types.{BasicType, NodeShape, ScalarType, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.{MleapContext, frame}
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}
import org.apache.spark.sql.mleap.TypeConverters
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec
import resource.managed
import ml.combust.mleap.core.util.VectorConverters
import ml.combust.mleap.xgboost.runtime.testing.FloatingPointApproximations
import org.apache.spark.ml.linalg.{SparseVector, Vectors}


/**
  * Created by hollinwilkins on 9/16/17.
  */
class XGBoostRuntimeClassificationModelParitySpec extends FunSpec
  with FloatingPointApproximations {

  private final val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "binary:logistic",
    "num_round" -> 15,
    "num_classes" -> 2
  )

  private final val TrainDataFilePath = "datasources/agaricus.train"
  private final val TestDataFilePath = "datasources/agaricus.test"
  private var mleapSchema: Option[StructType] = None

  private lazy val denseDataset: DMatrix =
    new DMatrix(this.getClass.getClassLoader.getResource(TrainDataFilePath).getFile)

  private lazy val leapFrameLibSVMtrain = leapFrameFromLibSVMFile(TrainDataFilePath)
  private lazy val leapFrameLibSVMtest = leapFrameFromLibSVMFile(TestDataFilePath)
  private lazy val numFeatures: Int =
    leapFrameLibSVMtrain.schema.getField("features").get.dataType.asInstanceOf[TensorType].dimensions.get.head

  def leapFrameFromLibSVMFile(filePath: String): DefaultLeapFrame = {

    // Use Spark utils to load libsvm from disk
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("XGBoostRuntimeClassificationModelParitySpec")
      .getOrCreate()

    // This is the dataset used by dmls-XGBoost https://github.com/dmlc/xgboost/blob/master/demo/data/agaricus.txt.train
    val dataFrame = spark.read.format("libsvm")
      .load(this.getClass.getClassLoader.getResource(filePath).getFile)

    mleapSchema = Option(TypeConverters.sparkSchemaToMleapSchema(dataFrame))

    val mleapMatrix: Array[ArrayRow] = dataFrame.collect().map {
      r => ArrayRow(
        Seq(
          r.get(0),
          VectorConverters.sparkVectorToMleapTensor(r.get(1).asInstanceOf[SparseVector])
        ))
    }

    DefaultLeapFrame(mleapSchema.get, mleapMatrix)
  }

  def trainBooster(xgboostParams: Map[String, Any], dataset: DMatrix): Booster =
    XGBoost.train(dataset, xgboostParams, xgboostParams("num_round").asInstanceOf[Int])

  def createBoosterClassifier(booster: Booster): Transformer ={

    XGBoostClassification(
        "xgboostSingleThread",
        NodeShape.probabilisticClassifier(
          rawPredictionCol = Some("raw_prediction"),
          probabilityCol = Some("probability")),
        XGBoostClassificationModel(XGBoostBinaryClassificationModel(booster, numFeatures, 0))
      )
  }

  def serializeModelToMleapBundle(transformer: Transformer): File = {
    import ml.combust.mleap.runtime.MleapSupport._

    new File("/tmp/mleap/spark-parity").mkdirs()
    val file = new File(s"/tmp/mleap/spark-parity/${classOf[XGBoostRuntimeClassificationModelParitySpec].getName}.zip")
    file.delete()

    for(bf <- managed(BundleFile(file))) {
      transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
    }
    file
  }

  def loadMleapTransformerFromBundle(bundleFile: File)
                                    (implicit context: MleapContext): frame.Transformer = {

    import ml.combust.mleap.runtime.MleapSupport._

    (for(bf <- managed(BundleFile(bundleFile))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  def equalityTestRowByRow(booster: Booster, mleapTransformer: Transformer) = {

    import XgbConverters._

    leapFrameLibSVMtest.dataset.foreach {
      r=>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(mleapSchema.get, Seq(r))).get

        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get
        val mleapRawPredictionColIndex = mleapResult.schema.indexOf("raw_prediction").get
        val mleapProbabilityColIndex = mleapResult.schema.indexOf("probability").get

        val singleRowDMatrix = r(1).asInstanceOf[SparseTensor[Double]].asXGB

        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head(0)

        val boosterProbability = Vectors.dense(1 - boosterResult, boosterResult).toDense
        val boosterPrediction = Math.round(boosterResult)


        assert (boosterPrediction == mleapResult.dataset.head.getDouble(mleapPredictionColIndex))

        assert (
          almostEqualSequences(
            Seq(boosterProbability.values),
            Seq(mleapResult.dataset.head.getTensor[Double](mleapProbabilityColIndex).toArray)
          )
        )

        val boosterResultWithMargin = booster.predict(singleRowDMatrix, true, 0).head(0)
        val boosterRawPrediction = Vectors.dense(- boosterResultWithMargin, boosterResultWithMargin).toDense

        assert (almostEqualSequences(
          Seq(boosterRawPrediction.values),
          Seq(mleapResult.dataset.head.getTensor[Double](mleapRawPredictionColIndex).toArray)
        ))
    }
  }

  it("Results between the XGBoost4j booster and the MLeap Transformer are the same") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterClassifier(trainBooster(xgboostParams, denseDataset))

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    equalityTestRowByRow(booster, deserializedTransformer)
  }

  it("has the correct inputs and outputs with columns: prediction, probability and raw_prediction") {

    val booster = trainBooster(xgboostParams, denseDataset)
    val transformer = createBoosterClassifier(booster)

    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("raw_prediction", TensorType(BasicType.Double, Seq(2))),
        StructField("probability", TensorType(BasicType.Double, Seq(2))),
        StructField("prediction", ScalarType.Double.nonNullable)))
  }

  it("Results are the same pre and post serialization") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterClassifier(booster)

    val preSerializationResult = xgboostTransformer.transform(leapFrameLibSVMtrain)

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)

    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)
    val deserializedModelResult = deserializedTransformer.transform(leapFrameLibSVMtrain).get

    assert(preSerializationResult.get.dataset == deserializedModelResult.dataset)
  }
}
