import sys

import pyspark.ml

import mleap.pyspark
import mleap.pyspark.feature
from mleap.pyspark.feature.string_map import StringMap
from mleap.pyspark.feature.math_binary import MathBinary
from mleap.pyspark.feature.math_unary import MathUnary

sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.pyspark.feature']

sys.modules['pyspark.ml'].mleap = mleap
sys.modules['pyspark.ml'].mleap.feature = sys.modules['mleap.pyspark.feature']
sys.modules['pyspark.ml'].mleap.feature.StringMap = StringMap
sys.modules['pyspark.ml'].mleap.feature.MathUnary = MathUnary
sys.modules['pyspark.ml'].mleap.feature.MathBinary = MathBinary
