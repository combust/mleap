package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.{MultinomialLabelerModel, ReverseStringIndexerModel}
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.{LeapFrame, LocalDataset, Row}
import ml.combust.mleap.tensor.Tensor
import org.scalatest.FunSpec

/**
  * Created by hollinwilkins on 1/18/17.
  */
class MultinomialLabelerSpec extends FunSpec {

  val schema = StructType(Seq(StructField("test_vec", TensorType(BasicType.Double)))).get
  val dataset = LocalDataset(Seq(Row(Tensor.denseVector(Array(0.0, 10.0, 20.0)))))
  val frame = LeapFrame(schema, dataset)
  val transformer = MultinomialLabeler(featuresCol = "test_vec",
    probabilitiesCol = "probs",
    labelsCol = "labels",
    model = MultinomialLabelerModel(9.0, ReverseStringIndexerModel(Seq("hello1", "world2", "!3"))))


  describe("#transform") {
    it("outputs the labels and probabilities for all classes with a probability greater than the threshold") {
      val frame2 = for(f <- transformer.transform(frame);
                       f2 <- f.select("probs", "labels")) yield f2

      assert(frame2.isSuccess)
      val data = frame2.get.dataset

      assert(data(0).getSeq[Double](0) == Seq(10.0, 20.0))
      assert(data(0).getSeq[String](1) == Seq("world2", "!3"))
    }
  }

  describe("#getFields") {
    it("has the correct inputs and outputs") {
      assert(transformer.getFields().get ==
        Seq(StructField("test_vec", TensorType(BasicType.Double)),
          StructField("probs", ListType(BasicType.Double)),
          StructField("labels", ListType(BasicType.String))))
    }
  }
}
