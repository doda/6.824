/**
 * Example Express app demonstrating SemperBench SDK usage
 */

import express from 'express';
import { SemperBench } from '@semperbench/sdk';

const app = express();
const PORT = process.env.PORT || 3000;

// Initialize SemperBench
const semperbench = new SemperBench({
  apiKey: process.env.SEMPERBENCH_API_KEY || 'test-api-key',
  endpoint: process.env.SEMPERBENCH_ENDPOINT || 'http://localhost:8787/v1/batch',
  sampleRate: 1.0, // Profile 100% of requests in development
  debug: true,
});

// Add SemperBench middleware FIRST (to capture all requests)
app.use(semperbench.middleware());

// JSON body parser
app.use(express.json());

// Routes
app.get('/', (req, res) => {
  res.json({
    message: 'Welcome to SemperBench Example App',
    endpoints: [
      'GET /',
      'GET /fast',
      'GET /slow',
      'GET /memory-heavy',
      'GET /cpu-heavy',
      'POST /api/users',
    ],
  });
});

// Fast endpoint (~10ms)
app.get('/fast', (req, res) => {
  res.json({ message: 'Fast response!', timestamp: Date.now() });
});

// Slow endpoint (~500ms)
app.get('/slow', async (req, res) => {
  await sleep(500);
  res.json({ message: 'Slow response!', timestamp: Date.now() });
});

// Memory-heavy endpoint (allocates 10MB)
app.get('/memory-heavy', (req, res) => {
  const data = new Array(10_000_000).fill('x');
  res.json({ message: 'Memory-heavy operation completed', size: data.length });
});

// CPU-heavy endpoint (calculates prime numbers)
app.get('/cpu-heavy', (req, res) => {
  const limit = 100000;
  const primes = calculatePrimes(limit);
  res.json({ message: 'CPU-heavy operation completed', primes: primes.length });
});

// Simulated database query
app.get('/api/users', async (req, res) => {
  // Simulate DB query latency
  await sleep(Math.random() * 200 + 50); // 50-250ms

  const users = [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' },
    { id: 3, name: 'Charlie' },
  ];

  res.json(users);
});

// Create user (with validation)
app.post('/api/users', async (req, res) => {
  const { name, email } = req.body;

  if (!name || !email) {
    return res.status(400).json({ error: 'Name and email are required' });
  }

  // Simulate DB write
  await sleep(Math.random() * 100 + 50);

  res.status(201).json({
    id: Math.floor(Math.random() * 1000),
    name,
    email,
    createdAt: new Date().toISOString(),
  });
});

// Simulate regression (endpoint gets slower over time)
let requestCount = 0;
app.get('/regression-demo', async (req, res) => {
  requestCount++;

  // Gradually increase latency every 100 requests
  const baseLatency = 50;
  const additionalLatency = Math.floor(requestCount / 100) * 50;
  const totalLatency = baseLatency + additionalLatency;

  await sleep(totalLatency);

  res.json({
    message: 'This endpoint gets slower over time!',
    requestCount,
    latencyMs: totalLatency,
  });
});

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptime: process.uptime(),
    memory: process.memoryUsage(),
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

// Error handler
app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
  console.error('Error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(PORT, () => {
  console.log(`\n🚀 Example app running on http://localhost:${PORT}`);
  console.log(`\n📊 SemperBench is profiling your app!`);
  console.log(`   Sample rate: ${(semperbench as any).config.sampleRate * 100}%`);
  console.log(`   Endpoint: ${(semperbench as any).config.endpoint}\n`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down...');
  await semperbench.shutdown();
  process.exit(0);
});

// Helper functions
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function calculatePrimes(limit: number): number[] {
  const primes: number[] = [];
  for (let i = 2; i <= limit; i++) {
    if (isPrime(i)) {
      primes.push(i);
    }
  }
  return primes;
}

function isPrime(n: number): boolean {
  if (n <= 1) return false;
  if (n <= 3) return true;
  if (n % 2 === 0 || n % 3 === 0) return false;
  for (let i = 5; i * i <= n; i += 6) {
    if (n % i === 0 || n % (i + 2) === 0) return false;
  }
  return true;
}
