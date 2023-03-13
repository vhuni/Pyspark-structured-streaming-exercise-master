from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp, window


spark = SparkSession \
    .builder \
    .appName("Streaming test") \
    .getOrCreate()

# define schema based on what we saw from the generated data
schema = StructType().add("data_value", "float").add("timestamp_string", "string")

streamDF = spark \
    .readStream \
    .option("cleanSource", "delete") \
    .schema(schema) \
    .format("csv") \
    .load("../data/streamtest")

one_minute_average_DF = streamDF.withColumn("timestamp", to_timestamp(streamDF.timestamp_string)) \
                    .withWatermark("timestamp", "15 seconds") \
                    .groupBy(window("timestamp", "1 minute", "5 second")) \
                    .avg().sort("window", ascending=False)

output_stream = one_minute_average_DF \
    .writeStream \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

output_stream.awaitTermination()