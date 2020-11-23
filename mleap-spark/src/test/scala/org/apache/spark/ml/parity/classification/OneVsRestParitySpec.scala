package org.apache.spark.ml.parity.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 4/12/17.
  */
class OneVsRestParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("fico_score_group_fnl", "dti")
  override val sparkTransformer: Transformer = new Pipeline().setStages(Array(new StringIndexer().
    setInputCol("fico_score_group_fnl").
    setOutputCol("fico_index"),
    new VectorAssembler().
      setInputCols(Array("fico_index", "dti")).
      setOutputCol("features"),
    new OneVsRest().setClassifier(new LogisticRegression()).
      setLabelCol("fico_index").
      setFeaturesCol("features").
      setPredictionCol("prediction"))).fit(dataset)

  override val unserializedParams = Set("stringOrderType", "classifier", "labelCol")

  override def assertModelTypesMatchTransformerTypes(model: Model, shape: NodeShape, exec: UserDefinedFunction): Unit = {
    /* OneVsRestModel intentionally does not have the same output schema as its exec */
    val inputFields = model.inputSchema.fields.map(_.name)
    val modelInputTypes = model.inputSchema.fields.map(_.dataType)
    val transformerInputTypes = exec.inputs.flatMap(_.dataTypes)
    checkTypes(modelInputTypes, transformerInputTypes, inputFields)
  }
}
