import sys

import pyspark.ml

import mleap.pyspark
import mleap.pyspark.feature
from mleap.pyspark.feature.string_map import StringMap
from mleap.pyspark.feature.math_binary import MathBinary
from mleap.pyspark.feature.math_unary import MathUnary

# These monkeypatches make it so mleap.feature appears to be in pyspark.ml
# This should be safe unless pyspark uses the namespace
# pyspark.ml.mleap (unlikely). Then we would mask those modules (undesirable).
sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.pyspark.feature']

sys.modules['pyspark.ml'].mleap = mleap
sys.modules['pyspark.ml'].mleap.feature = sys.modules['mleap.pyspark.feature']
sys.modules['pyspark.ml'].mleap.feature.StringMap = StringMap
sys.modules['pyspark.ml'].mleap.feature.MathUnary = MathUnary
sys.modules['pyspark.ml'].mleap.feature.MathBinary = MathBinary


class InteractionMaskedError(Exception):
    """
    If you got here, mleap is probably masking the pyspark Interaction
    import. How to fix this?

    Please remove the interaction.py file (and this code)
    because it's masking the pyspark Interaction import.
    """

try:
    # This is expected to fail for all pyspark < 3.0.0
    from pyspark.ml.feature import Interaction as PysparkInteraction
    # If instead it works and we get here, it means we probably managed
    # to upgrade to spark 3.0.0! Hurray
    # But hey, some more work to do..
    raise InteractionMaskedError()

except ImportError:
    # Spark provides Interaction only starting from version 3.0.0
    # I copy-pasted Interaction in mleap and monkeypatched it
    # to pretend it's in pyspark so that this line works:
    # https://github.com/apache/spark/blob/master/python/pyspark/ml/wrapper.py#L244
    from mleap.pyspark.feature.interaction import Interaction
    sys.modules['pyspark.ml'].feature.Interaction = Interaction
