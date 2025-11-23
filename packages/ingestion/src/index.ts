/**
 * SemperBench Ingestion Service (Cloudflare Worker)
 *
 * Receives metrics from SDK, validates, and writes to R2 as JSON-lines
 */

import { Hono } from 'hono';
import { cors } from 'hono/cors';

type Bindings = {
  BUCKET: R2Bucket;
  API_KEYS: KVNamespace;
};

const app = new Hono<{ Bindings: Bindings }>();

// CORS middleware
app.use('/*', cors());

// Health check
app.get('/health', (c) => {
  return c.json({ status: 'ok', service: 'semperbench-ingestion', version: '0.1.0' });
});

// Main ingestion endpoint
app.post('/v1/batch', async (c) => {
  try {
    // 1. Validate API key
    const apiKey = c.req.header('X-API-Key');
    if (!apiKey) {
      return c.json({ error: 'Missing X-API-Key header' }, 401);
    }

    // TODO: Validate API key against KV store
    // const isValid = await c.env.API_KEYS.get(apiKey);
    // if (!isValid) {
    //   return c.json({ error: 'Invalid API key' }, 401);
    // }

    // 2. Parse body
    let body: any;
    try {
      body = await c.req.json();
    } catch (err) {
      return c.json({ error: 'Invalid JSON' }, 400);
    }

    // 3. Validate schema
    if (!body.metrics || !Array.isArray(body.metrics)) {
      return c.json({ error: 'Invalid schema: metrics array required' }, 400);
    }

    if (body.metrics.length === 0) {
      return c.json({ accepted: true, count: 0 }, 202);
    }

    // 4. Extract metadata
    const customerId = body.customerId || apiKey;
    const timestamp = Date.now();
    const date = new Date(timestamp);
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hour = String(date.getUTCHours()).padStart(2, '0');

    // 5. Convert to JSON-lines format
    const jsonl = body.metrics.map((m: any) => JSON.stringify(m)).join('\n') + '\n';

    // 6. Write to R2
    const key = `raw/year=${year}/month=${month}/day=${day}/hour=${hour}/` +
                `customer_id=${customerId}/${timestamp}-${randomId()}.jsonl`;

    await c.env.BUCKET.put(key, jsonl, {
      httpMetadata: {
        contentType: 'application/x-ndjson',
      },
      customMetadata: {
        customerId,
        batchTimestamp: String(timestamp),
        count: String(body.metrics.length),
        sdkVersion: body.sdkVersion || 'unknown',
      },
    });

    // 7. Return success
    return c.json(
      {
        accepted: true,
        count: body.metrics.length,
        key,
      },
      202
    );
  } catch (error) {
    console.error('Ingestion error:', error);
    return c.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      500
    );
  }
});

// Batch status endpoint (for debugging)
app.get('/v1/batch/:key', async (c) => {
  const key = c.req.param('key');
  const apiKey = c.req.header('X-API-Key');

  if (!apiKey) {
    return c.json({ error: 'Missing X-API-Key header' }, 401);
  }

  try {
    const object = await c.env.BUCKET.get(key);

    if (!object) {
      return c.json({ error: 'Batch not found' }, 404);
    }

    return c.json({
      key,
      size: object.size,
      uploaded: object.uploaded,
      metadata: object.customMetadata,
    });
  } catch (error) {
    return c.json({ error: 'Failed to fetch batch' }, 500);
  }
});

/**
 * Generate random ID for batch files
 */
function randomId(): string {
  return Math.random().toString(36).substring(2, 15);
}

export default app;
