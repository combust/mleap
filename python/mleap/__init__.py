import mleap.feature
import pyspark.ml
import sys

sys.modules['pyspark.ml'].mleap = mleap
sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.feature']