package ml.combust.mleap.runtime

import ml.combust.mleap.runtime.frame

/**
  * Created by hollinwilkins on 10/6/17.
  */
package object transformer {
  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.Transformer instead", "MLeap 0.9.0")
  type Transformer = frame.Transformer

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.BaseTransformer instead", "MLeap 0.9.0")
  type BaseTransformer = frame.BaseTransformer

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.SimpleTransformer instead", "MLeap 0.9.0")
  type SimpleTransformer = frame.SimpleTransformer

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.MultiTransformer instead", "MLeap 0.9.0")
  type MultiTransformer = frame.MultiTransformer
}
