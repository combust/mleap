package ml.dmlc.xgboost4j.scala.spark.mleap

import java.io.File

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.types.{ScalarType, StructField, StructType}
import ml.combust.mleap.runtime.frame
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import resource.managed

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTable(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)

class XGBoostRegressionModelParitySpec extends FunSpec
  with BeforeAndAfterAll {
  val spark = SparkSession.builder().
    master("local[2]").
    appName("XGBoostRegressionModelParitySpec").
    getOrCreate()

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "reg:linear",
    "early_stopping_rounds" ->2,
    "num_round" -> 15
  )

  val dataset: DataFrame = {
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .map(line => PowerPlantTable(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble))
      .toDF
  }

  val sparkTransformer: Transformer = {
    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("AT", "V", "AP", "RH"))
      .setOutputCol("features")
    val regressor = new XGBoostRegressor(xgboostParams).
      setFeaturesCol("features").
      setLabelCol("PE").
      setPredictionCol("prediction").
      fit(featureAssembler.transform(dataset)).
      setLeafPredictionCol("leaf_prediction").
      setContribPredictionCol("contrib_prediction").
      setTreeLimit(2)

    SparkUtil.createPipelineModel(Array(featureAssembler, regressor))
  }

  def equalityTest(sparkDataset: DataFrame,
                   mleapDataset: DefaultLeapFrame): Unit = {
    val sparkPredictionCol = sparkDataset.schema.fieldIndex("prediction")
    val mleapPredictionCol = mleapDataset.schema.indexOf("prediction").get

    val sparkFeaturesCol = sparkDataset.schema.fieldIndex("features")
    val mleapFeaturesCol = mleapDataset.schema.indexOf("features").get

    val sparkCollected = sparkDataset.collect()
    val collected = mleapDataset.collect()

    sparkCollected.zip(collected).foreach {
      case (sp, ml) =>
        val v1 = sp.getDouble(sparkPredictionCol)
        val v2 = ml.getDouble(mleapPredictionCol)

        assert(sp.getAs[Vector](sparkFeaturesCol).toDense.values sameElements ml.getTensor[Double](mleapFeaturesCol).toDense.rawValues)
        assert(Math.abs(v2 - v1) < 0.0001)
    }
  }

  var bundleCache: Option[File] = None

  def serializedModel(transformer: Transformer): File = {
    import ml.combust.mleap.spark.SparkSupport._

    implicit val sbc = SparkBundleContext.defaultContext.withDataset(transformer.transform(dataset))

    bundleCache.getOrElse {
      new File("/tmp/mleap/spark-parity").mkdirs()
      val file = new File(s"/tmp/mleap/spark-parity/${classOf[XGBoostRegressionModelParitySpec].getName}.zip")
      file.delete()

      for(bf <- managed(BundleFile(file))) {
        transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
      }

      bundleCache = Some(file)
      file
    }
  }

  def mleapTransformer(transformer: Transformer)
                      (implicit context: SparkBundleContext): frame.Transformer = {
    import ml.combust.mleap.runtime.MleapSupport._

    (for(bf <- managed(BundleFile(serializedModel(transformer)))) yield {
      bf.loadMleapBundle().get.root
    }).tried.get
  }

  private val mleapSchema = StructType(StructField("AT", ScalarType.Double),
    StructField("V", ScalarType.Double),
    StructField("AP", ScalarType.Double),
    StructField("RH", ScalarType.Double)).get

  it("produces the same results") {
    val data = dataset.collect().map {
      r => Row(r.getDouble(0), r.getDouble(1), r.getDouble(2), r.getDouble(3))
    }
    val frame = DefaultLeapFrame(mleapSchema, data)
    val mleapT = mleapTransformer(sparkTransformer)
    val sparkDataset = sparkTransformer.transform(dataset)
    val mleapDataset = mleapT.transform(frame).get

    equalityTest(sparkDataset, mleapDataset)
  }
}
