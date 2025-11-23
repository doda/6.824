"""
SemperBench Compaction Job

Converts JSON-lines files from S3 to Parquet format for efficient querying.
Run this as a cron job every 10-15 minutes.
"""

import duckdb
import os
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "semperbench-data")


def compact_to_parquet():
    """
    Compact JSON-lines files to Parquet format
    """
    logger.info("Starting compaction job...")

    # Connect to DuckDB
    con = duckdb.connect(database=":memory:", read_only=False)

    # Install and configure S3 extension
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{AWS_REGION}';")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")

    # Calculate time range (last hour)
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    hour = now.strftime("%H")

    # Input pattern: raw JSON-lines files
    json_pattern = f"s3://{S3_BUCKET}/raw/year={year}/month={month}/day={day}/hour={hour}/**/*.jsonl"

    # Output path: partitioned Parquet
    parquet_base = f"s3://{S3_BUCKET}/parquet/metrics/year={year}/month={month}/day={day}/"

    logger.info(f"Reading from: {json_pattern}")
    logger.info(f"Writing to: {parquet_base}")

    try:
        # Check if there are files to process
        check_query = f"""
            SELECT COUNT(*) as file_count
            FROM read_ndjson('{json_pattern}', auto_detect=true, ignore_errors=true)
        """

        result = con.execute(check_query).fetchone()
        file_count = result[0] if result else 0

        if file_count == 0:
            logger.info("No files to process. Exiting.")
            return

        logger.info(f"Found {file_count} records to compact")

        # Compact JSON → Parquet
        # DuckDB will automatically partition by customer_id
        compact_query = f"""
            COPY (
                SELECT
                    traceId as trace_id,
                    CAST(timestamp AS BIGINT) as timestamp,
                    method,
                    endpoint,
                    CAST(durationMs AS DOUBLE) as duration_ms,
                    CAST(memoryMb AS DOUBLE) as memory_mb,
                    CAST(cpuPercent AS DOUBLE) as cpu_percent,
                    statusCode as status_code,
                    error,
                    metadata
                FROM read_ndjson('{json_pattern}', auto_detect=true, ignore_errors=true)
            )
            TO '{parquet_base}'
            (
                FORMAT PARQUET,
                PARTITION_BY (metadata->>'$.customerId'),
                COMPRESSION ZSTD,
                ROW_GROUP_SIZE 100000
            );
        """

        con.execute(compact_query)

        logger.info(f"✅ Successfully compacted {file_count} records to Parquet")

        # Optional: Delete raw JSON files after successful compaction
        # (Uncomment if you want to clean up after compaction)
        # delete_query = f"DELETE FROM 's3://{S3_BUCKET}/raw/year={year}/month={month}/day={day}/hour={hour}/**/*.jsonl';"
        # con.execute(delete_query)
        # logger.info("Deleted raw JSON files")

    except Exception as e:
        logger.error(f"❌ Compaction failed: {e}", exc_info=True)
        raise

    finally:
        con.close()

    logger.info("Compaction job completed")


def compact_historical(days_back: int = 7):
    """
    Backfill: Compact historical data for the last N days
    """
    logger.info(f"Starting historical compaction for last {days_back} days...")

    con = duckdb.connect(database=":memory:", read_only=False)

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{AWS_REGION}';")
    con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")

    for i in range(days_back):
        target_date = datetime.utcnow() - timedelta(days=i)
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")

        json_pattern = f"s3://{S3_BUCKET}/raw/year={year}/month={month}/day={day}/**/*.jsonl"
        parquet_base = f"s3://{S3_BUCKET}/parquet/metrics/year={year}/month={month}/day={day}/"

        logger.info(f"Processing {year}-{month}-{day}...")

        try:
            compact_query = f"""
                COPY (
                    SELECT
                        traceId as trace_id,
                        CAST(timestamp AS BIGINT) as timestamp,
                        method,
                        endpoint,
                        CAST(durationMs AS DOUBLE) as duration_ms,
                        CAST(memoryMb AS DOUBLE) as memory_mb,
                        CAST(cpuPercent AS DOUBLE) as cpu_percent,
                        statusCode as status_code,
                        error,
                        metadata
                    FROM read_ndjson('{json_pattern}', auto_detect=true, ignore_errors=true)
                )
                TO '{parquet_base}'
                (FORMAT PARQUET, PARTITION_BY (metadata->>'$.customerId'), COMPRESSION ZSTD);
            """

            con.execute(compact_query)
            logger.info(f"✅ Compacted {year}-{month}-{day}")

        except Exception as e:
            logger.error(f"❌ Failed to compact {year}-{month}-{day}: {e}")

    con.close()
    logger.info("Historical compaction completed")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        compact_historical(days)
    else:
        compact_to_parquet()
