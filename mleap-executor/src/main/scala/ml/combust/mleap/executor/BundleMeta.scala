package ml.combust.mleap.executor

import ml.combust.bundle.dsl.BundleInfo
import ml.combust.mleap.core.types.StructType

case class BundleMeta(info: BundleInfo,
                      inputSchema: StructType,
                      outputSchema: StructType)
