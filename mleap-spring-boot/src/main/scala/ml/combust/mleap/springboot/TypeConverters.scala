package ml.combust.mleap.springboot

import java.util.concurrent.TimeUnit

import com.google.protobuf.ProtocolStringList
import ml.combust.mleap.executor
import ml.combust.mleap.pb.Mleap

import scala.concurrent.duration.FiniteDuration

object TypeConverters {
  import scala.language.implicitConversions

  private val DEFAULT_TIMEOUT: FiniteDuration = FiniteDuration(5, TimeUnit.SECONDS)

  implicit def getTimeout(ms: Long): FiniteDuration = if (ms == 0) {
    DEFAULT_TIMEOUT
  } else { FiniteDuration(ms, TimeUnit.MILLISECONDS) }

  implicit def pbToMleapSelectMode(sm: Mleap.SelectMode): executor.SelectMode = {
    if (sm == Mleap.SelectMode.SELECT_MODE_RELAXED) {
      executor.SelectMode.Relaxed
    } else if (sm == Mleap.SelectMode.SELECT_MODE_STRICT) {
      executor.SelectMode.Strict
    } else { executor.SelectMode.Strict }
  }

  private implicit def pbToMleapSelect(select: ProtocolStringList): Option[Seq[String]] = {
    if (select.isEmpty) { None }
    else { Some(select.toArray().map(_.asInstanceOf[String]).toSeq)
    }
  }

  implicit def pbToMleapTransformOptions(options: Mleap.TransformOptions): executor.TransformOptions = {
        executor.TransformOptions(select = options.getSelectList, selectMode = options.getSelectMode)
  }
}
