package ml.combust.mleap.core.feature

import ml.combust.mleap.core.types.{BasicType, ListType, StructField, TensorType}
import org.apache.spark.ml.linalg.Vectors
import org.scalactic.TolerantNumerics
import org.scalatest.FunSpec

class WordToVectorModelSpec extends FunSpec {
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.000001)

  describe("word to vector model") {
    val model = WordToVectorModel(Map("test" -> 1), Array(12))

    it("has the right input schema") {
      assert(model.inputSchema.fields ==
        Seq(StructField("input", ListType(BasicType.String))))
    }

    it("has the right output schema") {
      assert(model.outputSchema.fields ==
        Seq(StructField("output", TensorType.Double(1))))
    }
  }

  describe("Sqrt kernel") {
    it("produces results using the sqrt kernel (division by sqrt(dot(vec, vec)))") {
      val hello = Vectors.dense(-0.02743354,  0.13925314, -0.41874424,  0.05635237, -1.01364303,
        0.13555442, -0.36437142,  0.10494551,  1.25634718,  0.74919909,
        -0.75405639,  0.34798685, -0.33082211, -1.83296537,  1.8524611 ,
        0.16053002,  0.05308712, -0.61047131, -2.04251647, -0.6457383 ,
        -0.06899478, -1.06984603,  1.81890905, -1.57762015, -1.14214861,
        -0.37704349, -1.13758969, -1.11241293, -0.01736556,  0.55350637,
        1.29117298,  0.6780861 ,  0.72507775,  0.38882053, -1.13152575)
      val there = Vectors.dense(0.05639598, -0.0189869 ,  0.01236993,  0.00477022, -0.10707449,
        0.02502576,  0.0702049 ,  0.07715208,  0.03785434,  0.06749821,
        0.0028507 ,  0.03143736, -0.07800865, -0.066576  ,  0.05038944,
        0.04129622,  0.05770208, -0.09861612, -0.02329824, -0.03803944,
        -0.01226865, -0.03243028,  0.05924392, -0.07248155, -0.03818463,
        0.03131858, -0.03253553,  0.04506788, -0.02503723, -0.03580079,
        0.05802456, -0.00171577, -0.07222789,  0.01021192,  0.01579604)
      val `{make}` = Vectors.dense(1.69664776, -0.9033435 , -1.13164949,  1.94182444, -0.53111398,
        2.28728724,  1.39580894,  1.38314795, -1.03503716,  1.0247947 ,
        -2.175174  ,  1.62514234, -0.64084077, -0.20218629, -0.0694286 ,
        0.37854579, -2.70390058, -2.27423668, -2.79813218, -0.46218753,
        0.77630186, -0.82613772,  1.18320072, -2.93088889,  0.6440177 ,
        -0.02956525, -1.51469374, -2.94850779, -0.89843947, -0.16953184,
        -1.4054004 , -1.22051024,  0.41841957,  0.26196802,  3.39272285)
      val wordVectors = Array(hello, there, `{make}`).flatMap(_.toArray)

      val model = WordToVectorModel(Map("hello" -> 0, "there" -> 1, "{make}" -> 2),
        wordVectors,
        kernel = WordToVectorKernel.Sqrt)

      val resultHello = model(Seq("hello"))
      val expectedHello = Vectors.dense(-0.00489383,  0.02484115, -0.07469912,  0.01005261, -0.18082216,
        0.02418134, -0.06499964,  0.01872106,  0.22411777,  0.13364843,
        -0.13451492,  0.06207682, -0.05901483, -0.32697977,  0.33045758,
        0.02863669,  0.00947013, -0.108901  , -0.36436126, -0.11519223,
        -0.01230787, -0.19084813,  0.32447228, -0.28142914, -0.20374607,
        -0.06726019, -0.20293281, -0.19844157, -0.00309781,  0.09873912,
        0.23033029,  0.1209627 ,  0.12934546,  0.06936107, -0.20185107)

      val resultSentence = model(Seq("hello", "there", "{make}", "qqq"))
      val expectedSentence = Vectors.dense(0.13878191, -0.06297886, -0.1236953 ,  0.16108668, -0.13284827,
        0.19686932,  0.0885994 ,  0.12588461,  0.02084325,  0.14810168,
        -0.23535359,  0.16121693, -0.08441966, -0.16903109,  0.14745265,
        0.04667632, -0.20855054, -0.23993334, -0.39118211, -0.09216406,
        0.05589835, -0.15509237,  0.24620885, -0.36842539, -0.04313309,
        -0.03018265, -0.21592611, -0.32297428, -0.07566708,  0.02800181,
        -0.00452011, -0.04376236,  0.08615666,  0.05316085,  0.18312679)
      for ((a, b) <- resultHello.toArray.zip(expectedHello.toArray)) { assert(a === b) }
      for ((a, b) <- resultSentence.toArray.zip(expectedSentence.toArray)) { assert(a === b) }
    }
  }
}
