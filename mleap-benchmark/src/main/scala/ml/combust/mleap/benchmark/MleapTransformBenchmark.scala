package ml.combust.mleap.benchmark

import java.io.File
import com.typesafe.config.Config
import ml.combust.mleap.runtime.serialization.{BuiltinFormats, FrameReader}
import org.scalameter.{Bench, Gen}

import java.nio.file.Path

/**
  * Created by hollinwilkins on 2/4/17.
  */
class MleapTransformBenchmark extends Benchmark {
  override def benchmark(config: Config): Unit = {
    val model = mleapBundleForPath(Path.of(config.getString("model-path"))).root
    val frame = FrameReader(BuiltinFormats.json).read(new File(config.getString("frame-path"))).get

    val start = config.getInt("start")
    val end = config.getInt("end")
    val step = config.getInt("step")
    object TransformBenchmark extends Bench.ForkedTime {
      val sizes: Gen[Int] = Gen.range("size")(start, end, step)
      val ranges: Gen[Range] = for(size <- sizes) yield 0 until size
      performance of "Range" in {
        measure method "transform" in {
          using(ranges) in {
            r => r.foreach {
              _ => model.transform(frame).get
            }
          }
        }
      }
    }

    TransformBenchmark.executeTests()
  }
}
