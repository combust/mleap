package ml.dmlc.xgboost4j.scala.spark.mleap

import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class PowerPlantTable(AT: Double, V : Double, AP : Double, RH : Double, PE : Double)

class XGBoostRegressionModelParitySpec extends SparkParityBase {
  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "reg:linear",
    "early_stopping_rounds" ->2,
    "num_round" -> 15,
    "nworkers" -> 2
  )

  override val dataset: DataFrame = {
    import spark.sqlContext.implicits._

    spark.sqlContext.sparkContext.textFile(this.getClass.getClassLoader.getResource("datasources/xgboost_training.csv").toString)
      .map(x => x.split(","))
      .filter(line => line(0) != "AT")
      .map(line => PowerPlantTable(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble))
      .toDF
  }

  override val sparkTransformer: Transformer = {
    new Pipeline().setStages(Array( new VectorAssembler()
      .setInputCols(Array("AT", "V", "AP", "RH"))
      .setOutputCol("features"),
      new XGBoostRegressor(xgboostParams).
        setFeaturesCol("features").
        setLabelCol("PE").
        setPredictionCol("prediction"))).fit(dataset)
  }

  override def equalityTest(sparkDataset: DataFrame,
                            mleapDataset: DataFrame): Unit = {
    val sparkPredictionCol = sparkDataset.schema.fieldIndex("prediction")
    val mleapPredictionCol = mleapDataset.schema.fieldIndex("prediction")

    val sparkFeaturesCol = sparkDataset.schema.fieldIndex("features")
    val mleapFeaturesCol = mleapDataset.schema.fieldIndex("features")


    sparkDataset.collect().zip(mleapDataset.collect()).foreach {
      case (sp, ml) =>
        val v1 = sp.getDouble(sparkPredictionCol)
        val v2 = ml.getDouble(mleapPredictionCol)

        assert(sp.getAs[Vector](sparkFeaturesCol) == ml.getAs[Vector](mleapFeaturesCol))

        assert(Math.abs(v2 - v1) < 0.0001)
    }
  }
}
