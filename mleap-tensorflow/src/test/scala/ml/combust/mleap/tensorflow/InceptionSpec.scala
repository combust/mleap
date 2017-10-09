//package ml.combust.mleap.tensorflow
//
//import java.nio.file.{FileSystems, Files}
//
//import ml.bundle.Socket.Socket
//import ml.combust.bundle.BundleFile
//import ml.combust.mleap.core.feature.MultinomialLabelerModel
//import ml.combust.mleap.runtime.transformer.Pipeline
//import ml.combust.mleap.runtime.transformer.feature.MultinomialLabeler
//import ml.combust.mleap.core.frame.{DefaultLeapFrame, Row}
//import ml.combust.mleap.runtime.types._
//import org.scalatest.FunSpec
//import resource._
//import ml.combust.mleap.runtime.MleapSupport._
//
//import scala.collection.JavaConverters._
//import scala.util.{Failure, Success}
//
///**
//  * Created by hollinwilkins on 1/20/17.
//  */
//class InceptionSpec extends FunSpec {
//  describe("the thing") {
//    it("does the thing") {
//      val image = Files.readAllBytes(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/inception-2015-12-05/cropped_panda.jpg")).toSeq
//      val schema = StructType(Seq(StructField("input_image", ListType(ByteType())))).get
//      val dataset = Seq(Row(image)))
//      val frame = DefaultLeapFrame(schema, dataset)
//
////      val graphBytes = Files.readAllBytes(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/booya.pb"))
////      val graph = new org.tensorflow.Graph()
////      graph.importGraphDef(graphBytes)
////      val model = TensorflowModel(graph,
////        Seq(("InputImage", ListType(ByteType()))),
////        Seq(("softmax_squeezed", TensorType(DoubleType()))),
////        None)
////      val transformer = TensorflowTransformer(uid = "inception_vgg16",
////        inputs = Seq(Socket("input_image", "InputImage")),
////        outputs = Seq(Socket("softmax_squeezed", "softmax_squeezed")),
////        model = model)
////
////      val labels = Files.readAllLines(FileSystems.getDefault.getPath("/Users/hollinwilkins/Downloads/inception-2015-12-05/labels.txt")).asScala
////      val labeler = MultinomialLabeler(uid = "labeler",
////        featuresCol = "softmax_squeezed",
////        probabilitiesCol = "probabilities",
////        labelsCol = "labels",
////        model = MultinomialLabelerModel(0.1, labels))
////      val pipeline = Pipeline(uid = "inception_pipeline", Seq(transformer, labeler))
//
////      for(bundle <- managed(BundleFile("jar:file:/tmp/mleap-inception.zip"))) {
////        pipeline.writeBundle.save(bundle)
////      }
//
//      for(bundle <- managed(BundleFile("jar:file:/tmp/mleap-inception.zip"))) {
//        val root = bundle.loadMleapBundle().get.root
//        root.transform(frame)
//        val t1 = System.nanoTime()
//        root.transform(frame)
//        val t2 = System.nanoTime()
//        println((t2 - t1) / 1000000)
//      }
//
////      frame2 match {
////        case Success(f2) =>
////          val map = f2.dataset(0).getSeq[Double](0).zip(f2.dataset(0).getSeq[String](1)).toMap
////          println(map.mkString("\n"))
////        case Failure(error) => error.printStackTrace()
////      }
//    }
//  }
//}
