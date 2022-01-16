package ml.combust.mleap.runtime.transformer.feature

import scala.reflect.runtime.universe.TypeTag

import ml.combust.mleap.core.feature.MapEntrySelectorModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}

case class MapEntrySelector[K: TypeTag, V: TypeTag](override val uid: String = Transformer.uniqueName("map_entry_selector"),
                     override val shape: NodeShape,
                     override val model: MapEntrySelectorModel[_, _]) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (m: Map[K, V], k: K) => model.asInstanceOf[MapEntrySelectorModel[K, V]](m, k)
}
