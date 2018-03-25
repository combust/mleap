package ml.combust.mleap.runtime.transformer.recommendation

import java.io.File
import java.net.URI

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.recommendation.ALSModel
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructField}
import ml.combust.mleap.runtime.test.TestUtil
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec
import resource.managed
import ml.combust.mleap.runtime.MleapSupport._

class ALSSpec extends FunSpec {

  val userFactors = Map(0 -> Vectors.dense(1, 2), 1 -> Vectors.dense(3, 1),
    2 -> Vectors.dense(2, 3))
  val itemFactors = Map(0 -> Vectors.dense(1, 2), 1 -> Vectors.dense(3, 1),
    2 -> Vectors.dense(2, 3), 3 -> Vectors.dense(1, 2))

  val transformer = ALS(shape = NodeShape().withInput("user", "user")
      .withInput("item", "movie").withOutput("prediction", "rating"),
    model = ALSModel(2, userFactors, itemFactors))

  describe("input/output schema") {
    it("has the correct inputs and outputs") {
      assert(transformer.schema.fields ==
        Seq(StructField("user", ScalarType.Int.nonNullable),
          StructField("movie", ScalarType.Int.nonNullable),
          StructField("rating", ScalarType.Float.nonNullable)))
    }
  }

  describe("serialization") {
    it("serializes ALS transformer correctly") {
      val uri = new URI(s"jar:file:${TestUtil.baseDir}/als.json.zip")
      for (file <- managed(BundleFile(uri))) {
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      val file = new File(s"${TestUtil.baseDir}/als.json.zip")
      val als = (for (bf <- managed(BundleFile(file))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get.asInstanceOf[ALS]

      assert(transformer.model.itemFactors sameElements als.model.itemFactors)
      assert(transformer.model.userFactors sameElements als.model.userFactors)
    }
  }
}
