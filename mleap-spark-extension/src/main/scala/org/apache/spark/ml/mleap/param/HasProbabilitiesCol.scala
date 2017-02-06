package org.apache.spark.ml.mleap.param

import org.apache.spark.ml.param.{Param, Params}

/**
  * Created by hollinwilkins on 1/18/17.
  */
private[ml] trait HasProbabilitiesCol extends Params {

  /**
    * Param for input column name.
    *
    * @group param
    */
  final val probabilitiesCol: Param[String] = new Param[String](this, "probabilitiesCol", "column to store probabilities in")

  /** @group getParam */
  final def getProbabilitiesCol: String = $(probabilitiesCol)
}

