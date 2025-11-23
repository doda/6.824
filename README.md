# SemperBench

**Continuous performance profiling with AI-powered optimization**

SemperBench automatically profiles your Node.js application, detects performance regressions, and uses AI agents to generate performance improvement PRs.

---

## 🎯 What It Does

1. **Profile**: SDK samples 1-5% of production traffic (low overhead)
2. **Detect**: Analytics engine identifies performance regressions (P95 > 20% increase)
3. **Fix**: AI agent generates PR with performance improvements
4. **Ship**: You review, approve, and merge

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Your Node.js App                       │
│  + @semperbench/sdk (1-5% sampling)                     │
└─────────────────┬───────────────────────────────────────┘
                  │ HTTPS POST (batched, gzipped)
                  ▼
┌─────────────────────────────────────────────────────────┐
│          Ingestion (Cloudflare Worker)                   │
│  Writes JSON-lines to S3/R2                             │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────┐
│               S3 Data Lake (Parquet)                     │
│  • raw/ (JSON-lines, 7 days)                            │
│  • parquet/ (columnar, 90 days)                         │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────┴─────────┬─────────────┐
        ▼                   ▼             ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Compaction   │    │  Analytics   │    │  AI Agent    │
│ (Cron job)   │    │  (FastAPI)   │    │ (Claude 3.5) │
│ JSON→Parquet │    │  + DuckDB    │    │ Opens PRs    │
└──────────────┘    └──────────────┘    └──────────────┘
```

---

## 📦 Monorepo Structure

```
semperbench/
├── packages/
│   ├── sdk/              # TypeScript SDK for Node.js
│   ├── ingestion/        # Cloudflare Worker (Hono)
│   └── analytics/        # FastAPI + DuckDB
├── apps/
│   └── example/          # Example Express app
├── package.json          # Root package.json (workspaces)
└── pnpm-workspace.yaml   # pnpm workspaces config
```

---

## 🚀 Quick Start

### Prerequisites

- **Node.js** 20+
- **pnpm** 8+
- **Python** 3.11+ (for analytics service)
- **AWS Account** (for S3) or **Cloudflare** (for R2)

### 1. Install Dependencies

```bash
# Install pnpm if needed
npm install -g pnpm

# Install all dependencies
pnpm install
```

### 2. Run the Example App

```bash
# Terminal 1: Run the example Express app
cd apps/example
pnpm dev

# The app will start on http://localhost:3000
```

### 3. Run the Ingestion Service (Local Dev)

```bash
# Terminal 2: Run the Cloudflare Worker locally
cd packages/ingestion
pnpm dev

# The ingestion endpoint will be available at http://localhost:8787
```

### 4. Run the Analytics Service

```bash
# Terminal 3: Run the FastAPI analytics service
cd packages/analytics
pip install -e .
python src/main.py

# The API will be available at http://localhost:8000
```

### 5. Generate Some Traffic

```bash
# Send requests to generate metrics
curl http://localhost:3000/fast
curl http://localhost:3000/slow
curl http://localhost:3000/memory-heavy
curl http://localhost:3000/cpu-heavy

# Or use a load testing tool
npm install -g autocannon
autocannon -c 10 -d 60 http://localhost:3000/api/users
```

---

## 📊 SDK Usage

### Basic Setup

```typescript
import express from 'express';
import { SemperBench } from '@semperbench/sdk';

const app = express();

// Initialize SemperBench
const semperbench = new SemperBench({
  apiKey: 'your-api-key',
  endpoint: 'https://ingest.semperbench.com/v1/batch',
  sampleRate: 0.05, // Profile 5% of requests
  batchSize: 100,   // Flush after 100 metrics
  flushInterval: 60_000, // Flush every 60 seconds
  debug: false,
});

// Add middleware (captures all HTTP requests)
app.use(semperbench.middleware());

// Your routes...
app.get('/api/users', async (req, res) => {
  const users = await db.getUsers();
  res.json(users);
});

app.listen(3000);
```

### Manual Profiling

```typescript
import { SemperBench, profile } from '@semperbench/sdk';

const semperbench = new SemperBench({ apiKey: 'your-api-key' });

// Profile a specific function
async function processData() {
  const result = await profile(semperbench, 'data-processing', async () => {
    // Your expensive operation
    return await heavyComputation();
  });

  return result;
}
```

### Fine-Grained Spans

```typescript
async function complexOperation() {
  const span1 = semperbench.startSpan('database-query');
  const users = await db.getUsers();
  span1.end(); // Logs duration

  const span2 = semperbench.startSpan('data-transformation');
  const transformed = users.map(transform);
  span2.end();

  return transformed;
}
```

---

## 🔧 Analytics API

### Get Metrics

```bash
# Get P50/P95/P99 latency for the last 7 days
curl "http://localhost:8000/api/metrics/customer-id?days=7"

# Response:
[
  {
    "hour": "2025-01-23T14:00:00",
    "endpoint": "/api/users",
    "p50": 120.5,
    "p95": 250.3,
    "p99": 350.1,
    "request_count": 1523
  }
]
```

### Detect Regression

```bash
# Check if an endpoint has regressed
curl "http://localhost:8000/api/regression/customer-id//api/users"

# Response:
{
  "customer_id": "customer-id",
  "endpoint": "/api/users",
  "baseline_p95": 150.0,
  "recent_p95": 220.0,
  "pct_change": 0.4667,
  "status": "REGRESSED"  # or "IMPROVED" or "STABLE"
}
```

### Find All Regressions

```bash
# Find all endpoints with >20% regression
curl "http://localhost:8000/api/regressions?threshold=0.2"
```

---

## 🗄️ Data Pipeline

### Ingestion → S3

The SDK sends batched metrics to the ingestion endpoint, which writes them to S3 as JSON-lines:

```
s3://semperbench-data/raw/
  year=2025/month=01/day=23/hour=14/
    customer_id=abc123/
      1737641200000-batch-001.jsonl
```

### Compaction (JSON → Parquet)

Run the compaction job every 10 minutes to convert JSON to Parquet:

```bash
cd packages/analytics
python src/compaction.py

# Or for historical backfill:
python src/compaction.py backfill 7  # Last 7 days
```

This creates partitioned Parquet files:

```
s3://semperbench-data/parquet/metrics/
  year=2025/month=01/day=23/
    customer_id=abc123.parquet
```

### Analytics with DuckDB

DuckDB queries Parquet files directly from S3 (no data download!):

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region='us-east-1';")

# Query 1 billion rows in seconds
result = con.execute("""
    SELECT endpoint, AVG(duration_ms) as avg_latency
    FROM read_parquet('s3://semperbench-data/parquet/metrics/**/*.parquet')
    WHERE customer_id = 'abc123'
    GROUP BY endpoint
    ORDER BY avg_latency DESC;
""").fetchdf()

print(result)
```

---

## 🤖 AI Agent (Coming Soon)

The AI agent will:

1. **Analyze** the regression (which endpoint, how much slower)
2. **Retrieve** relevant code from your repo (GitHub integration)
3. **Generate** hypotheses (N+1 query? Inefficient algorithm? Memory leak?)
4. **Create** fixes and validate in sandbox
5. **Open PR** with explanation and proof (benchmark results)

Example PR:

> **🚀 Performance Improvement: /api/users (45% faster)**
>
> **Problem**: N+1 query in user endpoint (baseline: 150ms → recent: 220ms)
>
> **Root Cause**: Fetching user roles in a loop instead of batch query
>
> **Solution**: Added `include: { roles: true }` to Prisma query
>
> **Proof**:
> - Before: P95 = 220ms
> - After: P95 = 120ms (45% improvement)

---

## 📈 Cost Analysis

**Scenario**: 10 customers, 10M requests/day, 1% sampled = 100K metrics/day

| Service | Cost |
|---------|------|
| S3 storage (300MB/month) | $0.01 |
| S3 PUT requests (100K/day) | $0.15 |
| Lambda compaction (4,320 runs/month) | $0.001 |
| Compute (DuckDB queries) | $20 |
| **Total** | **~$20/month** |

Compare to Kafka + ClickHouse: **$370-720/month** (18-36x more expensive!)

---

## 🚢 Deployment

### Cloudflare Workers (Ingestion)

```bash
cd packages/ingestion

# Deploy to production
wrangler deploy
```

### FastAPI (Analytics)

```bash
# Docker
cd packages/analytics
docker build -t semperbench-analytics .
docker run -p 8000:8000 semperbench-analytics

# Or Railway/Render
railway up
```

### Compaction Job (Cron)

Deploy as:
- AWS Lambda (with EventBridge cron trigger)
- Google Cloud Run Jobs
- Railway cron job
- Self-hosted cron

---

## 🔐 Environment Variables

### SDK

```bash
SEMPERBENCH_API_KEY=your-api-key
SEMPERBENCH_ENDPOINT=https://ingest.semperbench.com/v1/batch
```

### Analytics Service

```bash
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET=semperbench-data
```

### Cloudflare Worker

Configure in `wrangler.toml` or via Cloudflare dashboard.

---

## 🧪 Testing

```bash
# Run all tests
pnpm test

# Test SDK
cd packages/sdk
pnpm test

# Test analytics API
cd packages/analytics
pytest
```

---

## 📚 Tech Stack

| Component | Technology | Why? |
|-----------|-----------|------|
| **SDK** | TypeScript + OpenTelemetry | Industry standard, extensible |
| **Ingestion** | Cloudflare Workers + Hono | Global edge, zero cold starts |
| **Storage** | S3/R2 + Parquet | Cost-effective, columnar storage |
| **Analytics** | DuckDB + FastAPI | 10-100x faster than PostgreSQL |
| **Compaction** | Python + DuckDB | Converts JSON → Parquet |
| **AI Agent** | Claude 3.5 Sonnet | Best-in-class code generation |

---

## 🎯 Roadmap

### Phase 1: MVP (Weeks 1-8)
- [x] SDK with Express middleware
- [x] Ingestion service (Cloudflare Worker)
- [x] S3 data lake (JSON + Parquet)
- [x] Analytics API (FastAPI + DuckDB)
- [x] Compaction job
- [ ] Basic dashboard (Next.js)
- [ ] Regression detection (statistical)

### Phase 2: AI Agent (Weeks 9-16)
- [ ] GitHub integration
- [ ] Code analysis (AST + tracing)
- [ ] AI agent (Claude 3.5)
- [ ] PR generation
- [ ] Sandbox testing
- [ ] Eval system

### Phase 3: Scale (Weeks 17-24)
- [ ] Multi-framework support (Fastify, Next.js)
- [ ] Real-time alerts
- [ ] Team collaboration
- [ ] Self-service onboarding
- [ ] Enterprise features

---

## 🤝 Contributing

This is an early-stage project. Contributions welcome!

---

## 📄 License

MIT

---

## 💡 Why This Will Win

1. **Zero config**: `npm install @semperbench/sdk` → add middleware → done
2. **Low overhead**: 1-5% sampling, negligible performance impact
3. **AI that ships code**: Not just alerts, but actual fixes
4. **Cost-effective**: S3+Parquet+DuckDB is 18-36x cheaper than alternatives
5. **Developer-first**: Built by developers, for developers

**The first performance tool that makes your app faster, not just tells you it's slow.**

---

Built with ❤️ for Node.js developers who ship fast.
