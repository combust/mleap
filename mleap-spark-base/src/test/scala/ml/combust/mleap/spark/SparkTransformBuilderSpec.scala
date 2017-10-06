package ml.combust.mleap.spark

import java.util.UUID

import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.core.frame.TransformBuilder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructType}
import SparkSupport._
import ml.combust.mleap.core.{Model, types}
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructField}
import org.scalatest.FunSpec

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by hollinwilkins on 4/21/17.
  */
case class MyTransformer() extends Transformer {
  override val uid: String = UUID.randomUUID().toString

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(Seq("output1", "output2"), "input") {
      (input: Double) => (input + 23, input.toString)
    }
  }

  override val shape: NodeShape = NodeShape().withStandardInput("input").
    withOutput("output1", "output1").withOutput("output2", "output2")

  override val model: Model = new Model {
    override def inputSchema: types.StructType = types.StructType("input" -> ScalarType.Double).get

    override def outputSchema: types.StructType = types.StructType("output1" -> ScalarType.Double,
      "output2" -> ScalarType.String).get
  }
}

class SparkTransformBuilderSpec extends FunSpec {
  describe("transformer with multiple outputs") {
    it("works with Spark as well") {
      val spark = SparkSession.builder().
        appName("Spark/MLeap Parity Tests").
        master("local[2]").
        getOrCreate()
      val schema = new StructType().
        add("input", DoubleType)
      val data = Seq(Row(45.7d)).asJava
      val dataset = spark.createDataFrame(data, schema)
      val transformer = MyTransformer()
      val outputDataset = transformer.sparkTransform(dataset).collect()

      assert(outputDataset.head.getDouble(1) == 68.7)
      assert(outputDataset.head.getString(2) == "45.7")
    }
  }

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      val transformer = MyTransformer()
      assert(transformer.schema.fields ==
        Seq(StructField("input", types.ScalarType.Double),
          StructField("output1", types.ScalarType.Double),
          StructField("output2", types.ScalarType.String)))
    }
  }
}