package ml.combust.mleap.spark

import java.util.UUID

import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructType}
import SparkSupport._
import ml.combust.mleap.core
import ml.combust.mleap.core.types.{StringType, StructField}
import ml.combust.mleap.runtime.types
import org.scalatest.FunSpec

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 4/21/17.
  */
case class MyTransformer() extends Transformer {
  override val uid: String = UUID.randomUUID().toString

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutputs(Seq("output1", "output2"), "input") {
      (input: Double) => (input + 23, input.toString)
    }
  }

  override def getFields(): Try[Seq[StructField]] = {
    Success(Seq(core.types.StructField("input", core.types.DoubleType()),
      core.types.StructField("output1", core.types.DoubleType()),
      core.types.StructField("output2", StringType())))
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

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      val transformer = MyTransformer()
      assert(transformer.getFields().get ==
        Seq(StructField("input", core.types.DoubleType()),
          StructField("output1", core.types.DoubleType()),
          StructField("output2", core.types.StringType())))
    }
  }
}