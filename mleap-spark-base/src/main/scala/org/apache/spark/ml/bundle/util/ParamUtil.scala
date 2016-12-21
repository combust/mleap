package org.apache.spark.ml.bundle.util

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by hollinwilkins on 12/21/16.
  */
trait ParamUtil {
  def setOptional[T](obj1: Params,
                     obj2: Params,
                     param1: Param[T],
                     param2: Param[T]): Unit = {
    if(obj2.isSet(param2)) {
      obj1.set(param1, obj2.get(param2).get)
    } else {
      obj1.clear(param1)
    }
  }
}

object ParamUtil extends ParamUtil
