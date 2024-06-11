import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType, BinaryType, LongType

spark_jars_packages = ",".join(
        [
            "org.postgresql:postgresql:42.4.0",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        ]
)

# Способы безопасного хранения кредов мы в этом спринте не проходили, поэтому они просто выделены в отдельную структуру
# буду признателен за рекомендации, куда их сохранять в реальных кейсах помимо airflow connections
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

postgresql_sequrity_options = {
    'user': 'student',
    'password': 'de-student'
}

postgresql_sequrity_options_feedback = {
    'user': 'jovyan',
    'password': 'jovyan'
}


TOPIC_NAME_IN = 'student.topic.cohort23.yurgen001'
TOPIC_NAME_OUT = 'student.topic.cohort23.yurgen001.out'



# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df_with_feedback = df.withColumn("feedback", F.lit(None).cast(StringType()))

    (df_with_feedback.write
        .mode('append')
        .format('jdbc')
        .option('url', 'jdbc:postgresql://localhost:5432/de')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'public.subscribers_feedback')
        .options(**postgresql_sequrity_options_feedback) 
        .save()
    )


    # # создаём df для отправки в Kafka. Сериализация в json.
    value_json_struct = F.struct(
        F.col("restaurant_id"),
        F.col("adv_campaign_id"),
        F.col("adv_campaign_content"),
        F.col("adv_campaign_owner"),
        F.col("adv_campaign_owner_contact"),
        F.col("adv_campaign_datetime_start"),
        F.col("adv_campaign_datetime_end"),
        F.col("client_id"),
        F.col("datetime_created"),
        F.col("trigger_datetime_created")
    )

    df_serialized = (df.withColumn("value",F.to_json(value_json_struct))
                       .select("value")    
    )

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    (df_serialized.write    
        .format("kafka")
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
        .options(**kafka_security_options)
        .option("topic", TOPIC_NAME_OUT)
        .save()
    )

    # очищаем память от df
    df.unpersist()


if __name__ == '__main__':

    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = (
        SparkSession.builder
            .appName("RestaurantSubscribeStreamingService")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )

    # читаем из топика Kafka сообщения с акциями от ресторанов 
    adv_stream_df = (
        spark.readStream
            .format("kafka")
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .options(**kafka_security_options)
            .option('subscribe', TOPIC_NAME_IN)
            .load()
    )

    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id" , StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ])  


    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_adv_stream_df = (
        adv_stream_df
            .withColumn("parsed_value",
                        F.from_json(F.expr("CAST(value AS string)"),incomming_message_schema)
                    )
            .select("parsed_value.restaurant_id",
                    "parsed_value.adv_campaign_id",
                    "parsed_value.adv_campaign_content",
                    "parsed_value.adv_campaign_owner",
                    "parsed_value.adv_campaign_owner_contact",
                    "parsed_value.adv_campaign_datetime_start",
                    "parsed_value.adv_campaign_datetime_end",
                    "parsed_value.datetime_created",
                    "timestamp"
                    )
            .where("UNIX_TIMESTAMP(timestamp) BETWEEN adv_campaign_datetime_start AND adv_campaign_datetime_end")
            .drop_duplicates(["restaurant_id", "adv_campaign_id"])
            .withWatermark("timestamp", "1 day")
    )


    # вычитываем всех пользователей с подпиской на рестораны
    subscribers_restaurant_df = (
        spark.read
            .format('jdbc')
            .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')
            .option('driver', 'org.postgresql.Driver')
            .option('dbtable', 'public.subscribers_restaurants')
            .options(**postgresql_sequrity_options) 
            .load()
    )

    # # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.

    result_stream_df = (
        filtered_adv_stream_df.alias("adv")
            .join(subscribers_restaurant_df.alias("sub"), ["restaurant_id"], "inner")
            .selectExpr("adv.restaurant_id",
                        "adv.adv_campaign_id",
                        "adv.adv_campaign_content",
                        "adv.adv_campaign_owner",
                        "adv.adv_campaign_owner_contact",
                        "adv.adv_campaign_datetime_start",
                        "adv.adv_campaign_datetime_end",
                        "sub.client_id",
                        "adv.datetime_created",
                        "UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) AS trigger_datetime_created"
                        )
    )


    # запускаем стриминг
    (result_stream_df.writeStream 
        .foreachBatch(foreach_batch_function)
        .start()
        .awaitTermination()
    )