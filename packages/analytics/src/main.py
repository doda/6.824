"""
SemperBench Analytics Service

FastAPI service that queries performance metrics from S3/Parquet using DuckDB
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import duckdb
import os
from datetime import datetime, timedelta

app = FastAPI(
    title="SemperBench Analytics API",
    description="Query performance metrics and detect regressions",
    version="0.1.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DuckDB connection (persistent)
con = duckdb.connect(database=":memory:", read_only=False)

# Configure S3 access
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "semperbench-data")

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute(f"SET s3_region='{AWS_REGION}';")
con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY}';")
con.execute(f"SET s3_secret_access_key='{AWS_SECRET_KEY}';")


# Pydantic models
class MetricSummary(BaseModel):
    hour: datetime
    endpoint: str
    p50: float
    p95: float
    p99: float
    request_count: int


class RegressionResult(BaseModel):
    customer_id: str
    endpoint: str
    baseline_p95: float
    recent_p95: float
    pct_change: float
    status: str  # REGRESSED | IMPROVED | STABLE


@app.get("/")
def root():
    return {
        "service": "semperbench-analytics",
        "version": "0.1.0",
        "status": "ok",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/metrics/{customer_id}", response_model=List[MetricSummary])
def get_metrics(
    customer_id: str,
    days: int = Query(default=7, ge=1, le=90),
    endpoint: Optional[str] = None,
):
    """
    Get P50/P95/P99 latency metrics for a customer over the last N days
    """
    try:
        # Build query
        where_clauses = [
            f"customer_id = '{customer_id}'",
            f"timestamp >= EPOCH(NOW() - INTERVAL {days} DAYS)",
        ]

        if endpoint:
            where_clauses.append(f"endpoint = '{endpoint}'")

        where_clause = " AND ".join(where_clauses)

        query = f"""
            SELECT
                DATE_TRUNC('hour', to_timestamp(timestamp / 1000)) as hour,
                endpoint,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99,
                COUNT(*) as request_count
            FROM read_parquet('s3://{S3_BUCKET}/parquet/metrics/**/*.parquet')
            WHERE {where_clause}
            GROUP BY hour, endpoint
            ORDER BY hour DESC
            LIMIT 1000;
        """

        result = con.execute(query).fetchdf()

        # Convert to Pydantic models
        metrics = []
        for _, row in result.iterrows():
            metrics.append(
                MetricSummary(
                    hour=row["hour"],
                    endpoint=row["endpoint"],
                    p50=row["p50"],
                    p95=row["p95"],
                    p99=row["p99"],
                    request_count=row["request_count"],
                )
            )

        return metrics

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/regression/{customer_id}/{endpoint}", response_model=RegressionResult)
def detect_regression(customer_id: str, endpoint: str):
    """
    Detect if an endpoint has regressed (compare last 24h vs previous 7 days)
    """
    try:
        query = f"""
            WITH baseline AS (
                SELECT
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as baseline_p95
                FROM read_parquet('s3://{S3_BUCKET}/parquet/metrics/**/*.parquet')
                WHERE
                    customer_id = '{customer_id}'
                    AND endpoint = '{endpoint}'
                    AND timestamp BETWEEN
                        EPOCH(NOW() - INTERVAL 8 DAYS) * 1000 AND
                        EPOCH(NOW() - INTERVAL 1 DAYS) * 1000
            ),
            recent AS (
                SELECT
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as recent_p95
                FROM read_parquet('s3://{S3_BUCKET}/parquet/metrics/**/*.parquet')
                WHERE
                    customer_id = '{customer_id}'
                    AND endpoint = '{endpoint}'
                    AND timestamp >= EPOCH(NOW() - INTERVAL 1 DAYS) * 1000
            )
            SELECT
                '{customer_id}' as customer_id,
                '{endpoint}' as endpoint,
                baseline.baseline_p95,
                recent.recent_p95,
                (recent.recent_p95 - baseline.baseline_p95) / baseline.baseline_p95 as pct_change,
                CASE
                    WHEN recent.recent_p95 > baseline.baseline_p95 * 1.2 THEN 'REGRESSED'
                    WHEN recent.recent_p95 < baseline.baseline_p95 * 0.8 THEN 'IMPROVED'
                    ELSE 'STABLE'
                END as status
            FROM baseline, recent;
        """

        result = con.execute(query).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="No data found")

        return RegressionResult(
            customer_id=result[0],
            endpoint=result[1],
            baseline_p95=result[2],
            recent_p95=result[3],
            pct_change=result[4],
            status=result[5],
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/regressions", response_model=List[RegressionResult])
def detect_all_regressions(threshold: float = Query(default=0.2, ge=0.1, le=1.0)):
    """
    Find all endpoints with performance regressions (>threshold% P95 increase)
    """
    try:
        query = f"""
        WITH baseline AS (
            SELECT
                customer_id,
                endpoint,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as baseline_p95
            FROM read_parquet('s3://{S3_BUCKET}/parquet/metrics/**/*.parquet')
            WHERE timestamp BETWEEN
                EPOCH(NOW() - INTERVAL 8 DAYS) * 1000 AND
                EPOCH(NOW() - INTERVAL 1 DAYS) * 1000
            GROUP BY customer_id, endpoint
        ),
        recent AS (
            SELECT
                customer_id,
                endpoint,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as recent_p95
            FROM read_parquet('s3://{S3_BUCKET}/parquet/metrics/**/*.parquet')
            WHERE timestamp >= EPOCH(NOW() - INTERVAL 1 DAYS) * 1000
            GROUP BY customer_id, endpoint
        )
        SELECT
            recent.customer_id,
            recent.endpoint,
            baseline.baseline_p95,
            recent.recent_p95,
            (recent.recent_p95 - baseline.baseline_p95) / baseline.baseline_p95 as pct_change,
            'REGRESSED' as status
        FROM recent
        JOIN baseline USING (customer_id, endpoint)
        WHERE recent.recent_p95 > baseline.baseline_p95 * (1 + {threshold})
        ORDER BY pct_change DESC
        LIMIT 100;
        """

        result = con.execute(query).fetchall()

        regressions = []
        for row in result:
            regressions.append(
                RegressionResult(
                    customer_id=row[0],
                    endpoint=row[1],
                    baseline_p95=row[2],
                    recent_p95=row[3],
                    pct_change=row[4],
                    status=row[5],
                )
            )

        return regressions

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
