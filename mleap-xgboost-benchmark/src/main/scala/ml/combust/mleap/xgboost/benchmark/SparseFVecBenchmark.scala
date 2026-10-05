package ml.combust.mleap.xgboost.benchmark

import java.util.concurrent.TimeUnit

import com.yelp.xgboost.{FVec, Predictor}
import com.yelp.xgboost.parser.PredictorFactory
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.{DMatrix, XGBoost}
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.jdk.CollectionConverters._
import scala.util.Random

/**
  * Per-row cost of turning a SparseVector into an FVec and scoring it with the pure-JVM predictor,
  * for vectors of `width` features holding about 100 entries. Compares the map-backed FVec MLeap
  * used before 0.25.3 (absent index means missing), the dense row (absent index is 0.0, cost
  * proportional to `width`), and FVecFactory.fromSparseVector, which keeps the dense-row semantics
  * through FVec.fromSparse at a cost proportional to the stored entries.
  *
  * Run with `sbt "mleap-xgboost-benchmark/Jmh/run -i 5 -wi 3 -f 1 -t 1 .*SparseFVecBenchmark.*"`.
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@OperationsPerInvocation(SparseFVecBenchmark.Rows)
class SparseFVecBenchmark {

  @Param(Array("100", "8720", "100000", "1000000"))
  var width: Int = _

  var predictor: Predictor = _
  var rows: Array[SparseVector] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val random = new Random(0)
    predictor = PredictorFactory.fromModelBytes(SparseFVecBenchmark.train(width, random))
    rows = Array.fill(SparseFVecBenchmark.Rows)(SparseFVecBenchmark.row(width, random))
  }

  @Benchmark
  def previousMap(blackhole: Blackhole): Unit =
    rows.foreach(r => blackhole.consume(predictor.predict(SparseFVecBenchmark.mapFVec(r))))

  @Benchmark
  def denseRow(blackhole: Blackhole): Unit =
    rows.foreach(r => blackhole.consume(predictor.predict(FVec.fromArray(r.toArray))))

  @Benchmark
  def fvecFactory(blackhole: Blackhole): Unit =
    rows.foreach(r => blackhole.consume(predictor.predict(FVecFactory.fromSparseVector(r))))
}

object SparseFVecBenchmark {
  final val Rows = 256
  private val NonZeros = 100
  private val Informative = 20
  private val TrainingRows = 2000

  private def train(width: Int, random: Random): Array[Byte] = {
    val informative = Array.tabulate(Informative)(i => i * (width / Informative))
    val points = Iterator.fill(TrainingRows)(labeled(row(width, random, informative), informative))
    val params = Map("objective" -> "binary:logistic", "max_depth" -> 6, "tree_method" -> "exact")
    XGBoost.train(new DMatrix(points), params, 100).toByteArray("ubj")
  }

  private def labeled(vector: SparseVector, informative: Array[Int]): LabeledPoint = {
    val signal = informative.map(i => vector(i)).sum
    val label = if (signal > 0) 1.0f else 0.0f
    new LabeledPoint(label, vector.size, vector.indices, vector.values.map(_.toFloat))
  }

  private def row(width: Int, random: Random): SparseVector =
    row(width, random, Array.tabulate(Informative)(i => i * (width / Informative)))

  private def row(width: Int, random: Random, informative: Array[Int]): SparseVector = {
    val chosen = informative.filter(_ => random.nextBoolean()) ++
      Array.fill(NonZeros)(random.nextInt(width))
    val indices = chosen.distinct.sorted.take(NonZeros)
    Vectors.sparse(width, indices, indices.map(_ => random.nextGaussian())).toSparse
  }

  private def mapFVec(vector: SparseVector): FVec = {
    val entries = vector.indices.zip(vector.values.map(_.toFloat)).toMap
    FVec.fromMap(entries.map { case (k, v) => Int.box(k) -> Float.box(v) }.asJava)
  }
}
