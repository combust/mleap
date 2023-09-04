package ml.combust.mleap.benchmark

import java.io.File
import com.typesafe.config.Config
import ml.combust.bundle.BundleFile
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.sql.SparkSession
import org.scalameter._
import org.scalameter.Key._

import java.nio.file.Path
import scala.collection.JavaConverters._
import scala.util.Using

/**
  * Created by hollinwilkins on 2/4/17.
  */
class SparkTransformBenchmark extends Benchmark {
  override def benchmark(config: Config): Unit = {
    val model = Using(BundleFile(Path.of(config.getString("model-path")))) { bf =>
      bf.loadSparkBundle()
    }.flatten.get.root

    val spark = SparkSession.builder()
      .appName("Spark Benchmarks")
      .config("spark.ui.enabled", "false")
      .master("local[1]")
      .getOrCreate()

    val slowFrame = spark.sqlContext.read.format("avro").load(new File(config.getString("frame-path")).getAbsolutePath)
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
