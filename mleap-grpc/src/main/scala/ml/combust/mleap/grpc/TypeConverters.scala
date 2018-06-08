package ml.combust.mleap.grpc

import ml.combust.mleap.executor
import ml.combust.mleap.pb.{SelectMode, StreamConfig, TransformOptions}

import scala.concurrent.duration._

object TypeConverters {
  import scala.language.implicitConversions

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

  implicit def pbToMleapTransformOptions(options: Option[TransformOptions]): executor.TransformOptions = {
    options.map {
      options =>
        executor.TransformOptions(select = options.select, selectMode = options.selectMode)
    }
  }

  implicit def mleapToPbSelectMode(sm: executor.SelectMode): SelectMode = sm match {
    case executor.SelectMode.Strict => SelectMode.SELECT_MODE_STRICT
    case executor.SelectMode.Relaxed => SelectMode.SELECT_MODE_RELAXED
  }

  implicit def mleapToPbTransformOptions(options: executor.TransformOptions): TransformOptions = {
    TransformOptions(
      select = options.select.getOrElse(Seq()),
      selectMode = options.selectMode
    )
  }

  implicit def mleapToPbStreamConfig(config: executor.StreamConfig): StreamConfig = {
    StreamConfig(
      initTimeout = config.initTimeout.toMillis,
      inactivityTimeout = config.inactivityTimeout.toMillis,
      transformTimeout = config.transformTimeout.toMillis,
      parallelism = config.parallelism,
      bufferSize = config.bufferSize
    )
  }

  implicit def pbToMleapStreamConfig(config: StreamConfig): executor.StreamConfig = {
    executor.StreamConfig(
      initTimeout = config.initTimeout.millis,
      inactivityTimeout = config.inactivityTimeout.millis,
      transformTimeout = config.transformTimeout.millis,
      parallelism = config.parallelism,
      bufferSize = config.bufferSize
    )
  }
}
