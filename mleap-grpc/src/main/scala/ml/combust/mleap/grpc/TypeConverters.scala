package ml.combust.mleap.grpc

import ml.combust.mleap.executor
import ml.combust.mleap.pb.{SelectMode, TransformOptions}

object TypeConverters {
  import scala.language.implicitConversions

  implicit def pbToMleapSelectMode(sm: SelectMode): executor.SelectMode = {
    if (sm.isSelectModeRelaxed) {
      executor.SelectMode.Strict
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
}
