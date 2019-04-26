package org.apache.spark.ml.parity.feature

import org.apache.spark.ml.parity.SparkParityBase
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.DataFrame

/**
  * Created by hollinwilkins on 10/30/16.
  */
class ReverseStringIndexerParitySpec extends SparkParityBase {
  override val dataset: DataFrame = baseDataset.select("state")
  override val sparkTransformer: Transformer = {
    val stringIndexer = new StringIndexer().
      setInputCol("state").
      setOutputCol("state_index").
      fit(dataset)
    val reverseStringIndexer = new IndexToString().
      setInputCol("state_index").
      setOutputCol("state_reverse").
      setLabels(stringIndexer.labels)
    new Pipeline().setStages(Array(stringIndexer, reverseStringIndexer)).fit(dataset)
  }

//  override def extractSparkTransformerParamsToVerify(deserializedSparkModel: Transformer): Array[(Param[_], Param[_])] = {
//    sparkTransformer.asInstanceOf[PipelineModel].stages(1).params.zip(deserializedSparkModel.asInstanceOf[PipelineModel].stages(1).params)
//  }
}
