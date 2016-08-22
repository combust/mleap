package ml.combust.mleap.runtime.serialization.bundle

import java.io.File

import ml.combust.mleap.runtime.transformer.{Pipeline, Transformer}
import ml.bundle.BundleDef.BundleDef
import ml.bundle.dsl._
import ml.bundle.serializer._

/**
  * Created by hollinwilkins on 8/23/16.
  */
object MleapBundle {
  def readBundleDef(path: File)
                   (implicit registry: BundleRegistry): BundleDef = BundleSerializer(registry, path).readBundleDef()

  def readTransformerGraph(path: File)
                          (implicit registry: BundleRegistry): (Bundle, Pipeline) = {
    val bundle = BundleSerializer(registry, path).read()
    val pipeline = Pipeline(uid = bundle.name, transformers = bundle.nodes.map(_.asInstanceOf[Transformer]))

    (bundle, pipeline)
  }

  def readTransformer(path: File)
                     (implicit registry: BundleRegistry): (Bundle, Transformer) = {
    val bundle = BundleSerializer(registry, path).read()
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
                           (implicit registry: BundleRegistry): Unit = {
    val bundle = Bundle.createBundle(graph.uid, format, graph.transformers, list)
    BundleSerializer(registry, path).write(bundle)
  }

  def writeTransformer(transformer: Transformer,
                       path: File,
                       list: Option[AttributeList] = None,
                       format: SerializationFormat = SerializationFormat.Mixed)
                      (implicit registry: BundleRegistry): Unit = {
    transformer match {
      case transformer: Pipeline => writeTransformerGraph(transformer, path, list, format)
      case _ =>
        val bundle = Bundle.createBundle(transformer.uid,
          format,
          Seq(transformer),
          list)
        BundleSerializer(registry, path).write(bundle)
    }
  }
}
