from typing import List
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType,MapType
from pyspark.sql.functions import col,struct,when

from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               LongType,
                               BooleanType,
                               TimestampType,
                               ArrayType)

schema = {
    'raw_data': StructType([
        StructField("visit_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("user_id", LongType(), True),
        StructField("page", StructType([
        StructField("previous", StringType(), True),
        StructField("current", StringType(), True) ]),True),
        StructField("source", StructType([
        StructField("site", StringType(), True),
      StructField("api_version", StringType(), True) ]),True),
        StructField("user", StructType([
        StructField("latitude", LongType(), True),
      StructField("longitude", LongType(), True) ]),True),
        StructField("technical", StructType([
        StructField("browser", StringType(), True),
      StructField("os", StringType(), True),
      StructField("lang", StringType(), True),
      StructField("device", StructType([
        StructField("type", StringType(), True),
      StructField("version", StringType(), True) ]),True),
      StructField("network", StringType(), True) ]),True),
      StructField("keep_private", BooleanType(), True)
      ])
}
