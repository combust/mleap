package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{StructField, StructType, TensorType}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.tensorflow.converter.{MleapConverter, TensorflowConverter}
import org.tensorflow

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowModel(graph: tensorflow.Graph,
                           inputs: Seq[(String, TensorType)],
                           outputs: Seq[(String, TensorType)],
                           nodes: Option[Seq[String]] = None) extends Model with AutoCloseable {
  @transient
  private var session: Option[tensorflow.Session] = None

  def apply(values: Tensor[_] *): Seq[Any] = {
    val garbage: mutable.ArrayBuilder[tensorflow.Tensor[_]] = mutable.ArrayBuilder.make[tensorflow.Tensor[_]]()

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
    val s = session.getOrElse {
      session = Some(new tensorflow.Session(graph))
      session.get
    }

    f(s)
  }

  override def close(): Unit = {
    session.foreach(_.close())
    graph.close()
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
