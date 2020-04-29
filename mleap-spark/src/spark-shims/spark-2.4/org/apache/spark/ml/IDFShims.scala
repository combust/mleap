package org.apache.spark.ml

import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.feature.{IDFModel => OldIDFModel}

object IDFShims {

  def createIDFModel(uid: String, idf: Vector): IDFModel = {
    val oldModel = new OldIDFModel(OldVectors.fromML(idf))
    new IDFModel(uid = uid, idfModel = oldModel)
  }

}
