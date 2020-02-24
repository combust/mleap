package ml.combust.mleap.xgboost.runtime.testing

import ml.combust.mleap.core.types.{StructType, TensorType}
import ml.combust.mleap.core.util.VectorConverters
import ml.combust.mleap.runtime.frame.{ArrayRow, DefaultLeapFrame, Row}
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mleap.TypeConverters

trait CachedDatasetUtils {

  private final val TrainDataFilePath = "datasources/agaricus.train"
  private final val TrainDataMultinomialFilePath = "datasources/iris.scale.txt"

  val binomialDataset: DMatrix =
    new DMatrix(this.getClass.getClassLoader.getResource(TrainDataFilePath).getFile)

  val multinomialDataset: DMatrix =
    new DMatrix(this.getClass.getClassLoader.getResource(TrainDataMultinomialFilePath).getFile)

  lazy val leapFrameLibSVMtrain: DefaultLeapFrame = leapFrameFromLibSVMFile(TrainDataFilePath)
  lazy val leapFrameIrisTrain: DefaultLeapFrame = leapFrameFromLibSVMFile(TrainDataMultinomialFilePath)

  def numFeatures(dataset: DefaultLeapFrame): Int =
    dataset.schema.getField("features").get.dataType.asInstanceOf[TensorType].dimensions.get.head

  private def leapFrameFromLibSVMFile(filePath: String): DefaultLeapFrame = {

    // Use Spark utils to load libsvm from disk
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName(s"${this.getClass.getName}")
      .getOrCreate()

    // This is the dataset used by dmlc-XGBoost https://github.com/dmlc/xgboost/blob/master/demo/data/agaricus.txt.train
    val dataFrame = spark.read.format("libsvm")
      .load(this.getClass.getClassLoader.getResource(filePath).getFile)

    val mleapSchema = Option(TypeConverters.sparkSchemaToMleapSchema(dataFrame))

    val mleapMatrix: Array[ArrayRow] = dataFrame.collect().map {
      r => ArrayRow(
        Seq(
          r.get(0),
          VectorConverters.sparkVectorToMleapTensor(r.get(1).asInstanceOf[SparseVector])
        ))
    }

    DefaultLeapFrame(mleapSchema.get, mleapMatrix)
  }

  def toDenseFeaturesLeapFrame(sparseLeapFrame: DefaultLeapFrame): DefaultLeapFrame = {
    val featureColumnIndex = sparseLeapFrame.schema.indexOf("features").get
    val labelColumnIndex = sparseLeapFrame.schema.indexOf("label").get

    val denseDataset: Seq[Row] = sparseLeapFrame.dataset.map{
      row => {
        val array = new Array[Any](2)
        array(labelColumnIndex) = row.getDouble(labelColumnIndex)
        array(featureColumnIndex) = row.getTensor[Double](featureColumnIndex).toDense

        ArrayRow(array)
      }
    }

    DefaultLeapFrame(sparseLeapFrame.schema, denseDataset)
  }
}
