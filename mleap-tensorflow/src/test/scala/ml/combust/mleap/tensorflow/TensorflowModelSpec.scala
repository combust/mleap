package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.TensorType
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import org.scalatest.FunSpec
import org.tensorflow.ndarray.StdArrays
import org.tensorflow.ndarray.Shape
import org.tensorflow.op.core.Init
import org.tensorflow

import java.nio.file.Files
import org.tensorflow.types.TFloat32
import org.tensorflow.ConcreteFunction
import org.tensorflow.SavedModelBundle
import org.tensorflow.Signature

import scala.collection.JavaConverters._
import ml.combust.bundle.serializer.FileUtil
import ml.combust.mleap.tensorflow.converter.MleapConverter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.ZipOutputStream

/**
  * Created by hollinwilkins on 1/12/17.
  */
class TensorflowModelSpec extends FunSpec {

  describe("with an adding tensorflow model") {
    it("adds two floats together") {
      val graph = TestUtil.createAddGraph()
      val model = TensorflowModel(
        graph=Some(graph),
        inputs = Seq(("InputA", TensorType.Float()), ("InputB", TensorType.Float())),
        outputs = Seq(("MyResult", TensorType.Float())),
        modelBytes = graph.toGraphDef.toByteArray
      )

      assert(model(Tensor.scalar(23.4f), Tensor.scalar(45.6f)).head == Tensor.scalar(23.4f + 45.6f))
      assert(model(Tensor.scalar(42.3f), Tensor.scalar(99.9f)).head == Tensor.scalar(42.3f + 99.9f))
      assert(model(Tensor.scalar(65.8f), Tensor.scalar(34.6f)).head == Tensor.scalar(65.8f + 34.6f))

      model.close()
    }
  }

  describe("with a multiple tensorflow model") {
    describe("with a float and a float vector") {
      it("scales the float vector") {
        val graph = TestUtil.createMultiplyGraph()
        val model = TensorflowModel(
          inputs = Seq(("InputA", TensorType.Float()), ("InputB", TensorType.Float())),
          outputs = Seq(("MyResult", TensorType.Float(3))),
          modelBytes = graph.toGraphDef.toByteArray)
        val tensor1 = DenseTensor(Array(1.0f, 2.0f, 3.0f), Seq(3))
        val scale1 = Tensor.scalar(2.0f)

        assert(model(scale1, tensor1).head.asInstanceOf[DenseTensor[Float]].values sameElements Array(2.0f, 4.0f, 6.0f))

        model.close()
      }
    }
  }

  describe("with an tensorflow model with variables") {
    it("saved model") {
      var reducedSum = 0.0f
      val testFolder = Files.createTempDirectory("tf-saved-model-export")
      val input = DenseTensor(Array(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f), Seq(2, 3))
      val xyShape = Shape.of(2, 3L)
      val g = new tensorflow.Graph()
      val session = new tensorflow.Session(g)
      try {
        val tf = tensorflow.op.Ops.create(g)
        val signature = TestUtil.createGraphWithVariables(tf, xyShape)
        val f = tensorflow.ConcreteFunction.create(signature, session)
        session.run(Init.DEFAULT_NAME)
        val xTensor = MleapConverter.convert(input)
        val zTensor = f.call(xTensor).asInstanceOf[TFloat32]
        try {
          reducedSum = zTensor.getFloat()
          f.save(testFolder.toString)
        } finally {
          if (xTensor != null) xTensor.close()
          if (zTensor != null) zTensor.close()
        }
      } finally {
        session.close()
        g.close()
      }
      val bundle = SavedModelBundle.load(testFolder.toString)
      try {
        val signatureDef = bundle.metaGraphDef.getSignatureDefOrThrow(Signature.DEFAULT_KEY)
        val inputMap = signatureDef.getInputsMap.asScala.map{case (k, v) => (k, v.getName)}
        val outputMap = signatureDef.getOutputsMap.asScala.map{case (k,v) => (k, v.getName)}
        assert(inputMap("input") == "Placeholder:0")
        assert(outputMap("reducedSum") == "ReduceSum:0")
        val inputs = Seq(("Placeholder:0", TensorType.Float(2, 3)))
        val outputs = Seq(("ReduceSum:0", TensorType.Float()))
        val format  = Some("saved_model")
        val byteStream = new ByteArrayOutputStream()
        val zf = new ZipOutputStream(byteStream)
        FileUtil().zip(testFolder.toFile, zf)
        zf.close()
        val model = TensorflowModel(
          inputs = inputs,
          outputs = outputs,
          format = format,
          modelBytes = byteStream.toByteArray
        )
        val output = model(input)
        assert(reducedSum ==  output.head.asInstanceOf[DenseTensor[Float]](0))
      } finally if (bundle != null) bundle.close()
    }
  }
}
