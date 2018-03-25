package ml.combust.mleap.runtime.transformer.recommendation

import java.io.File
import java.net.URI

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.recommendation.ALSModel
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructField}
import ml.combust.mleap.runtime.test.TestUtil
import org.scalatest.FunSpec
import resource.managed
import ml.combust.mleap.runtime.MleapSupport._

class ALSSpec extends FunSpec {

  val userFactors = Map(0 -> Array(1.0f, 2.0f), 1 -> Array(3.0f, 1.0f), 2 -> Array(2.0f, 3.0f))
  val itemFactors = Map(0 -> Array(1.0f, 2.0f), 1 -> Array(3.0f, 1.0f), 2 -> Array(2.0f, 3.0f), 3 -> Array(1.0f, 2.0f))

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

      val (tfUsers, tfUserFactors) = transformer.model.userFactors.toSeq.unzip
      val (tfItems, tfItemFactors) = transformer.model.itemFactors.toSeq.unzip

      val (alsUsers, alsUserFactors) = als.model.userFactors.toSeq.unzip
      val (alsItems, alsItemFactors) = als.model.itemFactors.toSeq.unzip

      assert(tfUsers sameElements alsUsers)
      assert(tfItems sameElements alsItems)
      assert(tfUserFactors.flatten sameElements alsUserFactors.flatten)
      assert(tfItemFactors.flatten sameElements alsItemFactors.flatten)
    }
  }
}
