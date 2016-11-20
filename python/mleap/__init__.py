import mleap.feature
import mleap.sklearn
import pyspark.ml
import sklearn
import sys

sys.modules['sklearn'].mleap = mleap.sklearn
sys.modules['pyspark.ml'].mleap = mleap
sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.feature']
