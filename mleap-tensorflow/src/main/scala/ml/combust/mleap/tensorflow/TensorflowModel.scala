package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{StructField, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.tensorflow.converter.{MleapConverter, TensorflowConverter}
import org.tensorflow
import org.tensorflow.proto.framework.GraphDef

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowModel(@transient var graph: Option[tensorflow.Graph] = None,
                           @transient var session: Option[tensorflow.Session] = None,
                           inputs: Seq[(String, TensorType)],
                           outputs: Seq[(String, TensorType)],
                           nodes: Option[Seq[String]] = None,
                           graphBytes: Array[Byte]) extends Model with AutoCloseable {

  def apply(values: Tensor[_] *): Seq[Any] = {
    val garbage: mutable.ArrayBuilder[tensorflow.Tensor] = mutable.ArrayBuilder.make[tensorflow.Tensor]()

    val result = Try {
      val tensors = values.zip(inputs).map {
        case (v, (name, dataType)) =>
          val tensor = MleapConverter.convert(v, dataType)
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
              val mleapTensor = TensorflowConverter.convert(tensor, dataType)
              mleapTensor
          }
      }
    }

    garbage.result.foreach(_.close())

    result.get
  }

  private def withSession[T](f: (tensorflow.Session) => T): T = {
    val g = graph match {
      case Some(gg) => gg
      case _ => { // can also be null at deserialization time, not just empty
        val gg = new tensorflow.Graph()
        val graphDef = GraphDef.parseFrom(graphBytes)
        gg.importGraphDef(graphDef)
        graph = Some(gg)
        graph.get
      }
    }

    val s = session match {
      case Some(sess) => sess
      case _ => { // can also be null at deserialization time, not just empty
        session = Some(new tensorflow.Session(g))
        session.get
      }
    }

    f(s)
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
