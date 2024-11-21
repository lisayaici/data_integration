from pyspark.sql import DataFrame

def save_to_parquet(df: DataFrame, output_path: str, checkpoint_path: str):
    df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start()


