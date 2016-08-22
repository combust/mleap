package org.apache.spark.ml.mleap

import java.io.File

import ml.bundle.dsl._
import ml.bundle.serializer._
import org.apache.spark.ml.bundle.SparkRegistry
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 8/22/16.
  */
trait SparkSupport {
  implicit val sparkRegistry: BundleRegistry = SparkRegistry.instance

  implicit class TransformerOps(transformer: Transformer) {
    def serializeToBundle(path: File,
                          list: Option[AttributeList] = None,
                          format: SerializationFormat = SerializationFormat.Mixed)
                         (implicit registry: BundleRegistry): Unit = {
      SparkBundle.writeTransformer(transformer, path, list, format)(registry)
    }
  }

  object SparkBundle {
    def readTransformerGraph(path: File)
                            (implicit registry: BundleRegistry): PipelineModel = {
      val bundle = BundleSerializer(registry, path).read()
      new PipelineModel(uid = bundle.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
    }

    def readTransformer(path: File)
                       (implicit registry: BundleRegistry): Transformer = {
      val bundle = BundleSerializer(registry, path).read()
      if(bundle.nodes.length == 1) {
        bundle.nodes.head.asInstanceOf[Transformer]
      } else {
        new PipelineModel(uid = bundle.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
      }
    }

    def writeTransformerGraph(graph: PipelineModel,
                              path: File,
                              list: Option[AttributeList] = None,
                              format: SerializationFormat = SerializationFormat.Mixed)
                             (implicit registry: BundleRegistry): Unit = {
      val bundle = Bundle.createBundle(graph.uid, format, graph.stages, list)
      BundleSerializer(registry, path).write(bundle)
    }

    def writeTransformer(transformer: Transformer,
                         path: File,
                         list: Option[AttributeList] = None,
                         format: SerializationFormat = SerializationFormat.Mixed)
                        (implicit registry: BundleRegistry): Unit = {
      transformer match {
        case transformer: PipelineModel => writeTransformerGraph(transformer, path, list, format)(registry)
        case _ =>
          val bundle = Bundle.createBundle(transformer.uid, format, Seq(transformer), list)
          BundleSerializer(registry, path).write(bundle)
      }
    }
  }
}
object SparkSupport extends SparkSupport
