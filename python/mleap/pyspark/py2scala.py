from pyspark.ml.util import _jvm


def jvm_scala_object(jpkg, obj):
    """
    This accesses a scala object. For example, when passing:
        jpkg = _jvm().ml.combust.mleap.core.feature.UnaryOperation
        obj = Sin$
    it accesses:
        ml.combust.mleap.core.feature.UnaryOperation.Sin$.MODULE$

    or the scala case object `UnaryOperation.Sin`
        (for reference see file ml.combust.mleap.core.feature.MathUnaryModel)
    """
    return getattr(
        getattr(jpkg, obj),  # JavaClass
        "MODULE$",  # JavaObject
    )

def Some(value):
    """
    Instantiate a scala Some object. Useful when scala code takes in
    an Option[<value>]
    """
    return _jvm().scala.Some(value)

def ScalaNone():
    return jvm_scala_object(_jvm().scala, "None$")
