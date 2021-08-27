package ml.combust.mleap.tensorflow

import ml.combust.bundle.serializer.FileUtil
import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{StructField, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.tensorflow.converter.{MleapConverter, TensorflowConverter}
import org.tensorflow
import org.tensorflow.proto.framework.GraphDef

import java.io.ByteArrayInputStream
import java.nio.file.Files
import java.util.zip.ZipInputStream
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowModel( @transient var graph: Option[tensorflow.Graph] = None,
                            @transient var session: Option[tensorflow.Session] = None,
                           inputs: Seq[(String, TensorType)],
                           outputs: Seq[(String, TensorType)],
                           nodes: Option[Seq[String]] = None,
                           format: Option[String] = None,
                           modelBytes: Array[Byte]
                          ) extends Model with AutoCloseable {

  def apply(values: Tensor[_] *): Seq[Any] = {
    val garbage: mutable.ArrayBuilder[tensorflow.Tensor] = mutable.ArrayBuilder.make[tensorflow.Tensor]()

    val result = Try {
      val tensors = values.zip(inputs).map {
        case (v, (name, _)) =>
          val tensor = MleapConverter.convert(v)
          garbage += tensor
          (name, tensor)
      }

      withSession {
        session =>
          val runner = session.runner()

          tensors.foreach {
            case (name, tensor) => runner.feed(name, tensor)
          }

          outputs.foreach {
            case (name, _) => runner.fetch(name)
          }

          nodes.foreach {
            _.foreach {
              name => runner.addTarget(name)
            }
          }

          runner.run().asScala.zip(outputs).map {
            case (tensor, (_, dataType)) =>
              garbage += tensor
              TensorflowConverter.convert(tensor, dataType)
          }
      }
    }

    garbage.result.foreach(_.close())

    result.get
  }

  private def withSession[T](f: (tensorflow.Session) => T): T = {
    val (s,g) = (session, graph) match {
      case (Some(sess), Some(gg)) => (sess, gg)
      case _ => format match {
        case Some("graph") | None => getSessionFromFrozenGraph
        case Some("saved_model") => getSessionFromSavedModel
        case _ =>  throw new RuntimeException("Only graph and saved_model are supported")
      }
    }
    session = Some(s)
    graph = Some(g)
    f(s)
  }

  private def getSessionFromFrozenGraph: (tensorflow.Session, tensorflow.Graph) = {
    val graph = new tensorflow.Graph()
    graph.importGraphDef(GraphDef.parseFrom(modelBytes))
    (new tensorflow.Session(graph), graph)
  }

  private def getSessionFromSavedModel: (tensorflow.Session, tensorflow.Graph) = {
    val dest = Files.createTempDirectory("saved_model")
    val savedModelStream = new ZipInputStream(
      new ByteArrayInputStream(modelBytes)
    )
    FileUtil().extract(savedModelStream, dest.toFile)
    val modelBundle = tensorflow.SavedModelBundle.load(dest.toString)
    FileUtil().rmRF(dest.toFile)
    (modelBundle.session, modelBundle.graph)
  }

  override def close(): Unit = {
    session.foreach(_.close())
    graph.foreach(_.close())
  }

  override def finalize(): Unit = {
    close()
    super.finalize()
  }

  override def inputSchema: StructType = StructType(inputs.map {
    case (name, dt) => StructField(name, dt)
  }).get

  override def outputSchema: StructType = StructType(outputs.map {
    case (name, dt) => StructField(name, dt)
  }).get
}
