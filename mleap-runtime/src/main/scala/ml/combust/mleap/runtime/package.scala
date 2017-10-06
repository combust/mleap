package ml.combust.mleap

/**
  * Created by hollinwilkins on 10/6/17.
  */
package object runtime {
  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.LeapFrame instead", "MLeap 0.9.0")
  type LeapFrame[LF <: frame.LeapFrame[LF]] = frame.LeapFrame[LF]

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.DefaultLeapFrame instead", "MLeap 0.9.0")
  type DefaultLeapFrame = frame.DefaultLeapFrame

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.Row instead", "MLeap 0.9.0")
  type Row = frame.Row

  @deprecated("this type will be removed for MLeap 1.0, use ml.combust.mleap.runtime.frame.ArrayRow instead", "MLeap 0.9.0")
  type ArrayRow = frame.ArrayRow
}
