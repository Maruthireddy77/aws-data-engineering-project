from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aws_data_engineering.transformations import (
    add_quality_checks,
    create_silver_customers,
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("customer-data-pipeline")
        .master("local[*]")
        .getOrCreate()
    )


def read_customers(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .schema(CUSTOMER_SCHEMA)
        .csv(input_path)
    )


def main():
    spark = create_spark_session()

    input_path = "data/raw/customers.csv"

    customers_df = read_customers(spark, input_path)

    checked_df = add_quality_checks(customers_df)

    valid_df = checked_df.filter(
        F.col("reject_reason").isNull()
    )

    rejected_df = checked_df.filter(
        F.col("reject_reason").isNotNull()
    )

    silver_df = create_silver_customers(valid_df)

    print("VALID RECORDS")
    valid_df.show(truncate=False)

    print("REJECTED RECORDS")
    rejected_df.show(truncate=False)

    print("SILVER CUSTOMERS")
    silver_df.show(truncate=False)

    silver_df.printSchema()

    silver_df.write.mode("overwrite").parquet(
        "data/processed/customers"
    )

    spark.stop()

if __name__ == "__main__":
    main()