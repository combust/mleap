package org.apache.spark.ml.mleap.param

import org.apache.spark.ml.param.{BooleanParam, Params}

/**
  * Created by hollinwilkins on 5/10/16.
  */
/**
  * Trait for shared param dropLast.
  */
private[ml] trait HasDropLast extends Params {

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val dropLast: BooleanParam = new BooleanParam(this, "dropLast", "whether to drop the last column or not")

  /** @group getParam */
  final def getDropLast: Boolean = $(dropLast)
}
