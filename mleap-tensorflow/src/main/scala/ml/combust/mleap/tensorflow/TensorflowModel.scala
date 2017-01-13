package ml.combust.mleap.tensorflow

import java.io.Closeable

import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.tensorflow.converter.{MleapConverter, TensorflowConverter}
import org.tensorflow

import scala.collection.JavaConverters._

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowModel(graph: tensorflow.Graph,
                           inputs: Seq[(String, DataType)],
                           outputs: Seq[(String, DataType)],
                           nodes: Option[Seq[String]] = None) extends Closeable {
  @transient
  private var session: Option[tensorflow.Session] = None

  def apply(values: Any *): Seq[Any] = {
    val tensors = values.zip(inputs).map {
      case (v, (name, dataType)) => (name, MleapConverter.convert(v, dataType))
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
          case (tensor, (_, dataType)) => TensorflowConverter.convert(tensor, dataType)
        }
    }
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
}
