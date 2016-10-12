package ml.combust.mleap.runtime.bundle

import java.io.File

import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.combust.bundle.dsl._
import ml.combust.bundle.serializer._

/**
  * Created by hollinwilkins on 8/23/16.
  */
object MleapBundle {
  def readTransformerGraph(path: File)
                          (implicit hr: HasBundleRegistry): (Bundle, Pipeline) = {
    val bundle = BundleSerializer(path).read()
    val pipeline = Pipeline(uid = bundle.name, transformers = bundle.nodes.map(_.asInstanceOf[Transformer]))

    (bundle, pipeline)
  }

  def readTransformer(path: File)
                     (implicit hr: HasBundleRegistry): (Bundle, Transformer) = {
    val bundle = BundleSerializer(path).read()
    val model = if(bundle.nodes.length == 1) {
      bundle.nodes.head.asInstanceOf[Transformer]
    } else {
      Pipeline(uid = bundle.name, transformers = bundle.nodes.map(_.asInstanceOf[Transformer]))
    }

    (bundle, model)
  }

  def writeTransformerGraph(graph: Pipeline,
                            path: File,
                            list: Option[AttributeList] = None,
                            format: SerializationFormat = SerializationFormat.Mixed)
                           (implicit hr: HasBundleRegistry): Unit = {
    val bundle = Bundle.createBundle(graph.uid, format, graph.transformers, list)
    BundleSerializer(path).write(bundle)
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       list: Option[AttributeList] = None,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit hr: HasBundleRegistry): Unit = {
    transformer match {
      case transformer: Pipeline => writeTransformerGraph(transformer, path, list, format)
      case _ =>
        val bundle = Bundle.createBundle(transformer.uid,
          format,
          Seq(transformer),
          list)
        BundleSerializer(path).write(bundle)
    }
  }
}
