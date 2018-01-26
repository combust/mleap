package ml.combust.mleap.tensorflow

import java.nio.file.{FileSystems, Files}

import ml.combust.mleap.core.feature.MultinomialLabelerModel
import ml.combust.mleap.runtime.transformer.{Pipeline, PipelineModel}
import ml.combust.mleap.runtime.transformer.feature.MultinomialLabeler
import ml.combust.mleap.core.types.{NodeShape, StructType, TensorType}
import org.scalatest.{FunSpec, Ignore}
import resource._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

/**
  * Created by hollinwilkins on 1/20/17.
  */
@Ignore
class InceptionSpec extends FunSpec {
  describe("inception model") {
    it("properly analyzes the image") {
      val image = Files.readAllBytes(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/inception-2015-12-05/cropped_panda.jpg")).toSeq
      val imageTensor = DenseTensor(image.toArray, Seq())
      val schema = StructType("input_image" -> TensorType.Byte(-1)).get
      val dataset = Seq(Row(imageTensor))
      val frame = DefaultLeapFrame(schema, dataset)

      val graphBytes = Files.readAllBytes(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/booya.pb"))
      val graph = new org.tensorflow.Graph()
      graph.importGraphDef(graphBytes)
      val model = TensorflowModel(graph,
        inputs = Seq(("InputImage", TensorType.Byte(-1))),
        outputs = Seq(("softmax_squeezed", TensorType.Double(-1))))
      val shape = NodeShape().withInput("InputA", "input_a").
        withInput("InputImage", "input_image").
        withOutput("softmax_squeezed", "prediction")
      val transformer = TensorflowTransformer(uid = "inception",
        shape = shape,
        model = model)

      val labels = Files.readAllLines(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/inception-2015-12-05/labels.txt")).asScala
      val labeler = MultinomialLabeler(uid = "labeler",
        shape = NodeShape().withInput("features", "prediction").
          withOutput("probabilities", "probabilities").
          withOutput("labels", "labels"),
        model = MultinomialLabelerModel(0.1, labels))
      val pipeline = Pipeline(uid = "inception_pipeline",
        shape = NodeShape(),
        model = PipelineModel(Seq(transformer, labeler)))

      (for(frame2 <- pipeline.transform(frame);
           frame3 <- frame2.select("probabilities", "labels")) yield {
        val data = frame3.dataset
        val row = data.head
        val map = row.getSeq[Double](0).zip(row.getSeq[String](1)).toMap
        println(map.mkString("\n"))
      }).get

      //      val model = TensorflowModel(graph,
//        Seq(("InputImage", ListType(ByteType()))),
//        Seq(("softmax_squeezed", TensorType(DoubleType()))),
//        None)
//      val transformer = TensorflowTransformer(uid = "inception_vgg16",
//        inputs = Seq(Socket("input_image", "InputImage")),
//        outputs = Seq(Socket("softmax_squeezed", "softmax_squeezed")),
//        model = model)
//
//      val labels = Files.readAllLines(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/inception-2015-12-05/labels.txt")).asScala
//      val labeler = MultinomialLabeler(uid = "labeler",
//        featuresCol = "softmax_squeezed",
//        probabilitiesCol = "probabilities",
//        labelsCol = "labels",
//        model = MultinomialLabelerModel(0.1, labels))
//      val pipeline = Pipeline(uid = "inception_pipeline", Seq(transformer, labeler))

//      for(bundle <- managed(BundleFile("jar:file:/tmp/mleap-inception.zip"))) {
//        pipeline.writeBundle.save(bundle)
//      }

//      for(bundle <- managed(BundleFile("jar:file:/tmp/mleap-inception.zip"))) {
//        val root = bundle.loadMleapBundle().get.root
//        root.transform(frame)
//        val t1 = System.nanoTime()
//        root.transform(frame)
//        val t2 = System.nanoTime()
//        println((t2 - t1) / 1000000)
//      }

//      frame2 match {
//        case Success(f2) =>
//          val map = f2.dataset(0).getSeq[Double](0).zip(f2.dataset(0).getSeq[String](1)).toMap
//          println(map.mkString("\n"))
//        case Failure(error) => error.printStackTrace()
//      }
    }
  }
}
