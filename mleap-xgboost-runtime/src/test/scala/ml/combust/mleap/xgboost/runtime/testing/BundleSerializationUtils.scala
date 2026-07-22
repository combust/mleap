package ml.combust.mleap.xgboost.runtime.testing

import java.io.File
import java.nio.file.{Files, Path}

import ml.combust.bundle.BundleFile
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.runtime.{MleapContext, frame}
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.xgboost.runtime.bundle.ops.{XGBoostClassificationOp, XGBoostPredictorClassificationOp, XGBoostPredictorRegressionOp, XGBoostRegressionOp}
import scala.util.Using


trait BundleSerializationUtils {

  def serializeModelToMleapBundle(transformer: Transformer)(implicit context: MleapContext): File = {
    import ml.combust.mleap.runtime.MleapSupport._

    // The default registry now uses the predictor ops (reference.conf). Serializing the
    // booster-backed XGBoostClassification/XGBoostRegression fixtures resolves the op by node class,
    // so the booster ops must be registered for the store side.
    context.bundleRegistry.register(new XGBoostClassificationOp())
    context.bundleRegistry.register(new XGBoostRegressionOp())

    val tempDirPath = {
      val temp: Path = Files.createTempDirectory("xgboost-runtime-parity")
      temp.toFile.deleteOnExit()
      temp.toAbsolutePath
    }

    val file = new File(s"${tempDirPath}/${this.getClass.getName}.zip")

    Using(BundleFile(file)) { bf =>
      transformer.writeBundle.format(SerializationFormat.Json).save(bf).get
    }
    file
  }

  private def loadBundle(bundleFile: File)
                        (implicit context: MleapContext): frame.Transformer = {

    import ml.combust.mleap.runtime.MleapSupport._

    Using(BundleFile(bundleFile)) { bf =>
      bf.loadMleapBundle()
    }.flatten.get.root
  }

  /**
   * Loads via the xgboost4j Booster ops. reference.conf now defaults to the predictor ops, so the
   * booster ops are registered explicitly to exercise the JNI path (used as the parity reference).
   */
  def loadMleapTransformerFromBundle(bundleFile: File)
                                    (implicit context: MleapContext): frame.Transformer = {
    context.bundleRegistry.register(new XGBoostClassificationOp())
    context.bundleRegistry.register(new XGBoostRegressionOp())
    loadBundle(bundleFile)
  }

  /** Loads via the pure-JVM predictor ops (the default in reference.conf). */
  def loadXGBoostPredictorFromBundle(bundleFile: File)
                                    (implicit context: MleapContext): frame.Transformer = {
    context.bundleRegistry.register(new XGBoostPredictorClassificationOp())
    context.bundleRegistry.register(new XGBoostPredictorRegressionOp())
    loadBundle(bundleFile)
  }
}
