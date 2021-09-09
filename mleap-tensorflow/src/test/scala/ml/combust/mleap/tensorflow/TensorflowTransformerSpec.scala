package ml.combust.mleap.tensorflow

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.{FileUtil, SerializationFormat}
import ml.combust.mleap.core.types.{NodeShape, StructField, StructType, TensorType}
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import ml.combust.mleap.tensorflow.converter.MleapConverter
import org.scalatest.FunSpec
import org.tensorflow.ndarray.Shape
import org.tensorflow.types.TFloat32
import org.tensorflow.{SavedModelBundle, Signature}
import resource.managed

import java.io.{ByteArrayOutputStream, File}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.zip.ZipOutputStream
import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 1/13/17.
  */
class TensorflowTransformerSpec extends FunSpec {

  describe("with a scaling tensorflow model") {
    it("scales the vector using the model and returns the result") {
      val graph = TestUtil.createAddGraph()
      val model = TensorflowModel(
        inputs = Seq(("InputA", TensorType.Float()), ("InputB", TensorType.Float())),
        outputs = Seq(("MyResult", TensorType.Float())),
        modelBytes = graph.toGraphDef.toByteArray)
      val shape = NodeShape().withInput("InputA", "input_a").
        withInput("InputB", "input_b").
        withOutput("MyResult", "my_result")
      val transformer = TensorflowTransformer(uid = "tensorflow_ab",
        shape = shape,
        model = model)
      val schema = StructType(StructField("input_a", TensorType.Float()), StructField("input_b", TensorType.Float())).get
      val dataset = Seq(Row(Tensor.scalar(5.6f), Tensor.scalar(7.9f)),
        Row(Tensor.scalar(3.4f), Tensor.scalar(6.7f)),
        Row(Tensor.scalar(1.2f), Tensor.scalar(9.7f)))
      val frame = DefaultLeapFrame(schema, dataset)

      val data = transformer.transform(frame).get.dataset
      assert(data(0)(2) == Tensor.scalar(5.6f + 7.9f))
      assert(data(1)(2) == Tensor.scalar(3.4f + 6.7f))
      assert(data(2)(2) == Tensor.scalar(1.2f + 9.7f))

      transformer.close()
    }
  }

  describe("saved model format") {
    it("can create transformer and bundle from a TF saved model") {
      val testFolder = Files.createTempDirectory("tf-saved-model-export")
      val xyShape = Shape.of(2, 3L)
      val input = DenseTensor(Array(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f), Seq(2, 3))
      val f = TestUtil.createConcreteFunctionWithVariables(xyShape)
      val xTensor = MleapConverter.convert(input)
      val zTensor = f.call(xTensor).asInstanceOf[TFloat32]
      val reducedSum = zTensor.getFloat()
      try {
        f.save(testFolder.toString)
      } finally {
        if (f != null) f.close()
        if (xTensor != null) xTensor.close()
        if (zTensor != null) zTensor.close()
      }
      // load it back and export to bundle
      val bundle = SavedModelBundle.load(testFolder.toString)
      val byteStream = new ByteArrayOutputStream()
      try {
        val signatureDef = bundle.metaGraphDef.getSignatureDefOrThrow(Signature.DEFAULT_KEY)
        val inputMap = signatureDef.getInputsMap.asScala.map { case (k, v) => (k, v.getName) }
        val outputMap = signatureDef.getOutputsMap.asScala.map { case (k, v) => (k, v.getName) }

        val inputs = Seq((inputMap("input"), TensorType.Float(2, 3)))
        val outputs = Seq((outputMap("reducedSum"), TensorType.Float()))
        val format = Some("saved_model")
        val zf = new ZipOutputStream(byteStream)
        try FileUtil().zip(testFolder.toFile, zf) finally if (zf != null) zf.close()
        val model = TensorflowModel(
          inputs = inputs,
          outputs = outputs,
          format = format,
          modelBytes = byteStream.toByteArray
        )

        // transform using transformer
        val shape = NodeShape().
          withInput(inputMap("input"), "input_a").
          withOutput(outputMap("reducedSum"), "my_result")

        val transformer = TensorflowTransformer(uid = "tensorflow_saved_model", shape = shape, model = model)

        // serialization
        val uri = new URI(s"jar:file:${TestUtil.baseDir}/tensorflow_saved_model.json.zip")
        for (file <- managed(BundleFile(uri))) {
          transformer.writeBundle.name("bundle")
            .format(SerializationFormat.Json)
            .save(file)
        }

        // de-serialization
        val file = new File(s"${TestUtil.baseDir}/tensorflow_saved_model.json.zip")
        val tfTransformer = (for (bf <- managed(BundleFile(file))) yield {
          bf.loadMleapBundle().get.root
        }).tried.get.asInstanceOf[TensorflowTransformer]

        val schema = StructType(StructField("input_a", TensorType.Float(2, 3))).get
        val dataset = Seq(Row(input), Row(input), Row(input))
        val frame = DefaultLeapFrame(schema, dataset)

        // checks
        assert(transformer.inputSchema.fields equals (tfTransformer.inputSchema.fields))
        assert(transformer.outputSchema.fields equals (tfTransformer.outputSchema.fields))

        val actualData = tfTransformer.transform(frame).get.select("my_result").get.dataset
        assert(actualData(0)(0) == Tensor.scalar(reducedSum))
        assert(actualData(1)(0) == Tensor.scalar(reducedSum))
        assert(actualData(2)(0) == Tensor.scalar(reducedSum))
      } finally if (bundle != null) bundle.close()
    }
  }

  describe("example tensorflow wine quality model") {

    val graphBytes = Files.readAllBytes(Paths.get(getClass.getClassLoader.getResource("optimized_wine_quality.pb").getPath))


    it("can create transformer and bundle from a TF frozen graph") {

      val model = TensorflowModel(
        inputs = Seq(("dense_1_input", TensorType.Float(1, 11))),
        outputs = Seq(("dense_3/Sigmoid", TensorType.Float(1, 9))),
        modelBytes = graphBytes)
      val shape = NodeShape().withInput("dense_1_input", "features").withOutput("dense_3/Sigmoid", "score")
      val transformer = TensorflowTransformer(uid = "wine_quality", shape = shape, model = model)

      val frame = DefaultLeapFrame(StructType(StructField("features", TensorType.Float(1, 11))).get,
        Seq(Row(DenseTensor(Array(11f, 2.2f, 4.4f, 1.2f, 0.9f, 1.2f, 3.9f, 1.2f, 4.5f, 6.4f, 4.0f), Seq(1, 11)))))

      val expectedData = transformer.transform(frame).get.collect()

      // serialization
      val uri = new URI(s"jar:file:${TestUtil.baseDir}/tensorflow.json.zip")
      for (file <- managed(BundleFile(uri))) {
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      // de-serialization
      val file = new File(s"${TestUtil.baseDir}/tensorflow.json.zip")
      val tfTransformer = (for (bf <- managed(BundleFile(file))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get.asInstanceOf[TensorflowTransformer]

      // checks
      assert(transformer.inputSchema.fields sameElements (tfTransformer.inputSchema.fields))
      assert(transformer.outputSchema.fields sameElements (tfTransformer.outputSchema.fields))

      val actualData = tfTransformer.transform(frame).get.collect()
      assert(actualData.head.getTensor[Float](0).toArray.toSeq sameElements expectedData.head.getTensor[Float](0).toArray.toSeq)
      assert(actualData.head.getTensor[Float](1).toArray.toSeq sameElements expectedData.head.getTensor[Float](1).toArray.toSeq)
    }

    it("can create transformer and bundle from graph bytes") {
      val model = TensorflowModel(
        inputs = Seq(("dense_1_input", TensorType.Float(1, 11))),
        outputs = Seq(("dense_3/Sigmoid", TensorType.Float(1, 9))),
        modelBytes = graphBytes)
      val shape = NodeShape().withInput("dense_1_input", "features").withOutput("dense_3/Sigmoid", "score")
      val transformer = TensorflowTransformer(uid = "wine_quality", shape = shape, model = model)

      val uri = new URI(s"jar:file:${TestUtil.baseDir}/tensorflow2.json.zip")
      for (file <- managed(BundleFile(uri))) {
        transformer.writeBundle.name("bundle")
          .format(SerializationFormat.Json)
          .save(file)
      }

      val file = new File(s"${TestUtil.baseDir}/tensorflow2.json.zip")
      (for (bf <- managed(BundleFile(file))) yield {
        bf.loadMleapBundle().get.root
      }).tried.get.asInstanceOf[TensorflowTransformer]
    }
  }
}
