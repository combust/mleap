package ml.combust.mleap.runtime.transformer.feature

import java.io.File
import java.net.URI

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.feature.StandardScalerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.test.TestUtil
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec
import resource.managed
import ml.combust.mleap.runtime.MleapSupport._

class StandardScalerSpec extends FunSpec {

  val means = Some(Vectors.dense(Array(50.0, 20.0, 30.0)))
  val std = Some(Vectors.dense(Array(5.0, 1.0, 3.0)))

  val transformer = StandardScaler(shape = NodeShape.vector(3, 3),
    model = StandardScalerModel(std, means))

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(transformer.schema.fields ==
        Seq(StructField("input", TensorType.Double(3)),
          StructField("output", TensorType.Double(3))))
    }
  }

  describe("serialization") {
    it("serializes std as well as mean correctly") {
      val uri = new URI(s"jar:file:${TestUtil.baseDir}/standard-scaler.json.zip")
      for (file <- managed(BundleFile(uri))) {
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      val file = new File(s"${TestUtil.baseDir}/standard-scaler.json.zip")
      val scaler = (for (bf <- managed(BundleFile(file))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get.asInstanceOf[StandardScaler]

      assert(transformer.model.std sameElements scaler.model.std)
      assert(transformer.model.mean sameElements scaler.model.mean)
    }
  }
}