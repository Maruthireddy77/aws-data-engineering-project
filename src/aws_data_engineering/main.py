import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from aws_data_engineering.config import (
    INPUT_PATH,
    LOG_LEVEL,
    OUTPUT_PATH,
)
from aws_data_engineering.transformations import (
    add_quality_checks,
    create_silver_customers,
)

logger = logging.getLogger(__name__)

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
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    logger.info("Customer pipeline started")

    spark = None

    try:
        spark = create_spark_session()

        logger.info("Reading customer data from %s", INPUT_PATH)
        customers_df = read_customers(spark, INPUT_PATH)

        logger.info("Applying data quality checks")
        checked_df = add_quality_checks(customers_df)

        valid_df = checked_df.filter(
            F.col("reject_reason").isNull()
        )

        rejected_df = checked_df.filter(
            F.col("reject_reason").isNotNull()
        )

        logger.info("Creating Silver customer dataset")
        silver_df = create_silver_customers(valid_df)

        print("VALID RECORDS")
        valid_df.show(truncate=False)

        print("REJECTED RECORDS")
        rejected_df.show(truncate=False)

        print("SILVER CUSTOMERS")
        silver_df.show(truncate=False)

        silver_df.printSchema()

        logger.info(
            "Writing Silver customer data to %s",
            OUTPUT_PATH,
        )

        silver_df.write.mode("overwrite").parquet(OUTPUT_PATH)

        logger.info("Customer pipeline completed successfully")

    except Exception:
        logger.exception("Customer pipeline failed")
        raise

    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()


if __name__ == "__main__":
    main()