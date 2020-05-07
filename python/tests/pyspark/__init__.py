import logging

# prevent py4j from flooding the output with warnings upon failing tests
logging.getLogger("py4j").setLevel(logging.ERROR)
