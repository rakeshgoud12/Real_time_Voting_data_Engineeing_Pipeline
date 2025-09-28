#!/usr/bin/env python3
"""
spark-streaming.py — read votes from Kafka, aggregate, write aggregates back to Kafka.
Adjust KAFKA_BOOTSTRAP if running inside Docker (use 'broker:29092').
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

# ---------- CONFIG ----------
# <--- change to "broker:29092" if running inside Docker
KAFKA_BOOTSTRAP = 'localhost:9092'
VOTES_TOPIC = "votes_topic"
AGG_CAND_TOPIC = "aggregated_votes_per_candidate"
AGG_TURN_TOPIC = "aggregated_turnout_by_location"

CHECKPOINT_VOTES = "/Users/rakeshgoudedigi/Documents/new_downloads/Resume/Resum/New_Resume/DE_Resume/Overleaf_resume/DE_project/real_time_voting_project/checkpoints/checkpoints1"
CHECKPOINT_TURNOUT = "/Users/rakeshgoudedigi/Documents/new_downloads/Resume/Resum/New_Resume/DE_Resume/Overleaf_resume/DE_project/real_time_voting_project/checkpoints/checkpoints2"

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("ElectionAnalysis")
             .master("local[*]")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
             # optional: .config("spark.jars", "/path/to/postgresql-42.x.x.jar")
             .config("spark.sql.adaptive.enabled", "false")
             .getOrCreate())

    # ---- schema for incoming vote JSON messages ----
    vote_schema = StructType([
        StructField("voter_id", StringType(), True),
        StructField("candidate_id", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("voter_name", StringType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("biography", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("candidate_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postalcode", StringType(), True),
        ]), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    # ---- read votes topic as streaming dataframe ----
    votes_df = (spark.readStream
                .format("kafka")
                # <-- REQUIRED
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", VOTES_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) AS value")
                .select(from_json(col("value"), vote_schema).alias("data"))
                .select("data.*")
                )

    # ---- ensure types ----
    votes_df = (votes_df
                .withColumn("voting_time", col("voting_time").cast(TimestampType()))
                .withColumn("vote", col("vote").cast(IntegerType()))
                )

    # ---- watermark to handle late events ----
    enriched_votes_df = votes_df.withWatermark("voting_time", "1 minute")

    # ---- aggregations ----
    votes_per_candidate = (enriched_votes_df
                           .groupBy("candidate_id", "candidate_name", "party_affiliation", "photo_url")
                           .agg(_sum("vote").alias("total_votes"))
                           )

    turnout_by_location = (enriched_votes_df
                           .groupBy(col("address.state").alias("state"))
                           .count()
                           .withColumnRenamed("count", "total_votes")
                           )

    # ---- write aggregated results back to Kafka ----
    votes_per_candidate_to_kafka = (votes_per_candidate
                                    .selectExpr("to_json(struct(*)) AS value")
                                    .writeStream
                                    .format("kafka")
                                    # <-- REQUIRED
                                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                                    .option("topic", AGG_CAND_TOPIC)
                                    .option("checkpointLocation", CHECKPOINT_VOTES)
                                    .outputMode("update")
                                    .start())

    turnout_by_location_to_kafka = (turnout_by_location
                                    .selectExpr("to_json(struct(*)) AS value")
                                    .writeStream
                                    .format("kafka")
                                    # <-- REQUIRED
                                    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                                    .option("topic", AGG_TURN_TOPIC)
                                    .option("checkpointLocation", CHECKPOINT_TURNOUT)
                                    .outputMode("update")
                                    .start())

    # ---- wait for streams ----
    spark.streams.awaitAnyTermination()
# candidate_schema = StructType([])

# voter_schema = StructType([])

# read candidate data from postgres
"""candidates_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:15432/voting") \
        .option("dbtable", "candidates") \
        .option("user"), "postgre")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .load()"""

"""votes_df= spark
        .readstream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "voters_topic")
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value"), voter_schema).alias("data"))
        .select("data.*")"""

# Perform Joins
"""enriched_votes_df = votes_df.alias("vote").join(voters_df.alias("voter"),
                                                expr("vote.voter_id == voter.voter_id"), "inner")
                .join(candidates_df.alias("candidate"), expr("vote.candidate_id == candidate.candidate_id"), "inner")
        .select(
        col("vote.voter_id", col("voter.name").alias("voter_name"), col("vote.candidate_id"),
            col("voter.gender"),col("voting_time").cast(TimestampType()).alias("voting_time"),
            col("voter.address_city").alias("voter_city"),col("voter.address_state").alias("voter_state"),
            col("voter.address_country").alias("voter_country"),
            col("voter.address_postcode").alias("voter_postcode"),
            col("voter.registered.age").alias("voter_registered_age"),
            col("candidate.name").alias("candidate_name"),col("candidate.party_affiliation"))
    """
# Voter turnout by age
# turnout_by_age = enriched_votes_df.groupBy("registered_age").agg(count("*").alias("total_votes"))

# voter turnout by gender ( assuming gender data is available in voters_df)
# turnout_by_gender = enriched_votes_df.groupBy("gender").agg(count("*").alias("total_votes"))

# voter turnout by location

# party_wise_votes = enriched_votes_df.groupBy("party_affiliation").agg(count("*").alias("total_votes"))

#
