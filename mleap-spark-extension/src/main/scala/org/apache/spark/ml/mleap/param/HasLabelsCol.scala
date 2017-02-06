package org.apache.spark.ml.mleap.param

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by hollinwilkins on 1/18/17.
  */
private[ml] trait HasLabelsCol extends Params {

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val labelsCol: Param[String] = new Param[String](this, "labelsCol", "column to store labels in")

  /** @group getParam */
  final def getLabelsCol: String = $(labelsCol)
}
