package ml.combust.mleap.databricks.runtime.testkit

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import ml.combust.bundle.BundleFile
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.Pipeline

class TestXgboost(session: SparkSession) extends Runnable {
  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "binary:logistic",
    "early_stopping_rounds" ->2,
    "num_round" -> 15,
    "nworkers" -> 2
  )

  override def run(): Unit = {
    val sqlContext = session.sqlContext
    // Create a temporary file and copy the contents of the resource avro to it
    val path = Files.createTempFile("mleap-databricks-runtime-testkit", ".avro")
    Files.copy(getClass.getClassLoader.getResource("datasources/lending_club_sample.avro").openStream(),
      path,
      StandardCopyOption.REPLACE_EXISTING)

    val sampleData = sqlContext.read.format("avro").load(path.toString)
    sampleData.show()

    val stringIndexer = new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index")

    val featureAssembler = new VectorAssembler().
      setInputCols(Array(stringIndexer.getOutputCol, "dti", "loan_amount")).
      setOutputCol("features")

    val logisticRegression = new XGBoostClassifier(xgboostParams).
      setFeaturesCol("features").
      setLabelCol("approved").
      setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(stringIndexer, featureAssembler, logisticRegression))

    val model = pipeline.fit(sampleData)

    val modelPath = Files.createTempFile("mleap-databricks-runtime-testkit", ".zip")
    Files.delete(modelPath)

    {
      println("Writing model to...", modelPath)
      implicit val sbc = SparkBundleContext.defaultContext.withDataset(model.transform(sampleData))
      val bf = BundleFile(new File(modelPath.toString))
      model.writeBundle.save(bf).get
      bf.close()
    }

    {
      val bf = BundleFile(new File(modelPath.toString))
      bf.loadMleapBundle()
      bf.close()
    }
  }
}
