package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.RowTransformer

class RowTransformerSupport {

  def createRowTransformer(schema: StructType) = RowTransformer(schema)
}
