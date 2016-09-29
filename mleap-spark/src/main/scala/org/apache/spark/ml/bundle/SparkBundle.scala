package org.apache.spark.ml.bundle

import java.io.File

import ml.combust.bundle.dsl.{AttributeList, Bundle}
import ml.combust.bundle.serializer.{BundleSerializer, HasBundleRegistry, SerializationFormat}
import org.apache.spark.ml.{PipelineModel, Transformer}

/**
  * Created by hollinwilkins on 9/19/16.
  */
object SparkBundle {
  def readTransformerGraph(path: File)
                          (implicit hr: HasBundleRegistry): PipelineModel = {
    val bundle = BundleSerializer(path).read()
    new PipelineModel(uid = bundle.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
  }

  def readTransformer(path: File)
                     (implicit hr: HasBundleRegistry): (Bundle, Transformer) = {
    val bundle = BundleSerializer(path).read()
    val transformer = if(bundle.nodes.length == 1) {
      bundle.nodes.head.asInstanceOf[Transformer]
    } else {
      new PipelineModel(uid = bundle.name, stages = bundle.nodes.map(_.asInstanceOf[Transformer]).toArray)
    }

    (bundle, transformer)
  }

  def writeTransformerGraph(graph: PipelineModel,
                            path: File,
                            list: Option[AttributeList] = None,
                            format: SerializationFormat = SerializationFormat.Mixed)
                           (implicit hr: HasBundleRegistry): Unit = {
    val bundle = Bundle.createBundle(graph.uid, format, graph.stages, list)
    BundleSerializer(path).write(bundle)
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       list: Option[AttributeList] = None,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit hr: HasBundleRegistry): Unit = {
    transformer match {
      case transformer: PipelineModel => writeTransformerGraph(transformer, path, list, format)(hr)
      case _ =>
        val bundle = Bundle.createBundle(transformer.uid, format, Seq(transformer), list)
        BundleSerializer(path).write(bundle)
    }
  }
}