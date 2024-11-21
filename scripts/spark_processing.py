from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, lower
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Créer une session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

# 2. Configuration de Kafka
kafka_topic = "data-topic"
kafka_broker = "localhost:9092"

# 3. Lire les messages Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Convertir les données Kafka en texte
df_stream = df_stream.selectExpr("CAST(value AS STRING) as message")

# 4. Définir le schéma des messages Kafka
message_schema = StructType([
    StructField("SITE_ID", StringType(), True),       
    StructField("ph", FloatType(), True),           
    StructField("so4_ueq_l", FloatType(), True),     
    StructField("chla_ug_l", FloatType(), True)      
])

# 5. Parser les messages JSON
parsed_stream = df_stream.select(from_json(col("message"), message_schema).alias("data")).select("data.*")

# 6. Vérifier le schéma des données Kafka
print("Schema des données Kafka :")
parsed_stream.printSchema()

# 7. Lire les fichiers CSV nettoyés
methods_data_path = "/Users/lisayaici/Desktop/Projet_data_integration/data/raw/Methods_2022_8_1.csv"
site_info_data_path = "/Users/lisayaici/Desktop/Projet_data_integration/data/raw/Site_Information_2022_8_1.csv"

methods_df = spark.read.csv(methods_data_path, header=True, inferSchema=True, sep=";")
site_info_df = spark.read.csv(site_info_data_path, header=True, inferSchema=True, sep=";")

# 8. Vérifier les schémas des fichiers CSV
print("Schema de methods_df :")
methods_df.printSchema()

print("Schema de site_info_df :")
site_info_df.printSchema()

# 9. Normaliser les colonnes utilisées dans les jointures
parsed_stream = parsed_stream.withColumn("Parsed_SITE_ID", trim(lower(col("SITE_ID"))))
site_info_df = site_info_df.withColumn("Info_SITE_ID", trim(lower(col("SITE_ID"))))
methods_df = methods_df.withColumn("PROGRAM_ID", trim(lower(col("PROGRAM_ID"))))

# 10. Joindre les données avec Site_Information
enriched_stream = parsed_stream.join(
    site_info_df,
    parsed_stream.Parsed_SITE_ID == site_info_df.Info_SITE_ID,
    "left"
).drop("Info_SITE_ID")  



# 11. Joindre les données avec Method_Data si nécessaire
enriched_stream = enriched_stream.join(methods_df, "PROGRAM_ID", "left")

# 12. Vérifier les résultats avant écriture
query = enriched_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

# 13. Pistes de vérification des données nulles (facultatif mais utile pour déboguer)
# Affiche les valeurs nulles dans les DataFrames
methods_df.filter(col("PROGRAM_ID").isNull()).show()
site_info_df.filter(col("SITE_ID").isNull()).show()

# Affiche les valeurs distinctes des colonnes de jointure
parsed_stream.select("Parsed_SITE_ID").distinct().show()
site_info_df.select("Info_SITE_ID").distinct().show()

