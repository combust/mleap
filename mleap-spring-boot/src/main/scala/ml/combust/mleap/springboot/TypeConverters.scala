package ml.combust.mleap.springboot

import java.util.concurrent.TimeUnit

import com.google.protobuf.ProtocolStringList
import ml.combust.mleap.executor
import ml.combust.mleap.pb.{Mleap, SelectMode, TransformOptions}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

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

  implicit class RichFuture[T](f: Future[T]) {
    def mapAll[U](pf: PartialFunction[Try[T], U])(implicit executor: ExecutionContext): Future[U] = {
      val p = Promise[U]()
      f.onComplete(r => p.complete(Try(pf(r))))(executor)
      p.future
    }
  }

  implicit def pbToMleapSelectMode(sm: SelectMode): executor.SelectMode = {
    if (sm.isSelectModeRelaxed) {
      executor.SelectMode.Relaxed
    } else if (sm.isSelectModeStrict) {
      executor.SelectMode.Strict
    } else { executor.SelectMode.Strict }
  }

  private implicit def pbToMleapSelect(select: Seq[String]): Option[Seq[String]] = {
    if (select.isEmpty) { None }
    else { Some(select) }
  }

  implicit def pbToMleapTransformOptions(options: TransformOptions): executor.TransformOptions = {
    executor.TransformOptions(select = options.select, selectMode = options.selectMode)
  }
}
