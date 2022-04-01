from pyspark.sql.types import (IntegerType,
                               StringType,
                               DoubleType,
                               StructField,
                               StructType,
                               LongType,
                               BooleanType)

schema = {
    'raw_data': StructType([
        StructField("visit_id", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("user_id", LongType(), True),
        StructField("keep_private", BooleanType(), True)

    ])

}