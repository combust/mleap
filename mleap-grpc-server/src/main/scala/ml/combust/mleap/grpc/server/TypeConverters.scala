package ml.combust.mleap.grpc.server

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
}
