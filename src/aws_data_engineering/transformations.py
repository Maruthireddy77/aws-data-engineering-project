from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quality_checks(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "parsed_signup_date",
            F.expr("try_cast(signup_date AS date)"),
        )
        .withColumn(
            "reject_reason",
            F.when(
                F.col("customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID"),
            )
            .when(
                F.col("name").isNull(),
                F.lit("MISSING_NAME"),
            )
            .when(
                F.col("email").isNull(),
                F.lit("MISSING_EMAIL"),
            )
            .when(
                F.col("parsed_signup_date").isNull(),
                F.lit("INVALID_SIGNUP_DATE"),
            ),
        )
    )
def create_silver_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["customer_id"])
        .select(
            F.col("customer_id").cast("int").alias("customer_id"),
            F.trim(F.col("name")).alias("name"),
            F.trim(F.col("city")).alias("city"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            F.col("parsed_signup_date").alias("signup_date"),
        )
    )