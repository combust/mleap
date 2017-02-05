package ml.combust.mleap.benchmark

import java.io.File

import com.typesafe.config.Config
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.sql.SparkSession
import org.scalameter._
import com.databricks.spark.avro._
import org.scalameter.Key._

import scala.collection.JavaConverters._
import resource._

/**
  * Created by hollinwilkins on 2/4/17.
  */
class SparkTransformBenchmark extends Benchmark {
  override def benchmark(config: Config): Unit = {
    val model = (for(bf <- managed(BundleFile(new File(config.getString("model-path"))))) yield {
      bf.loadSparkBundle()
    }).tried.flatMap(identity).get.root

    val spark = SparkSession.builder().
      appName("Spark Benchmarks").
      master("local[1]").
      getOrCreate()

    val slowFrame = spark.sqlContext.read.avro(new File(config.getString("frame-path")).getAbsolutePath)
    val data = slowFrame.collect().toSeq
    val frame = spark.sqlContext.createDataFrame(data.asJava, slowFrame.schema)

    val start = config.getInt("start")
    val end = config.getInt("end")
    val step = config.getInt("step")
    val benchRuns = config.getInt("bench-runs")
    object TransformBenchmark extends Bench.LocalTime {
      override def warmer: Warmer = Warmer.Zero

      val sizes: Gen[Int] = Gen.range("size")(start, end, step)
      val ranges: Gen[Range] = for(size <- sizes) yield 0 until size
      performance of "Range" in {
        measure method "transform" config(exec.benchRuns -> benchRuns) in {
          using(ranges) in {
            r =>
              r.foreach {
              _ => model.transform(frame).collect()
            }
          }
        }
      }
    }

    TransformBenchmark.executeTests()

    spark.close()
  }
}
