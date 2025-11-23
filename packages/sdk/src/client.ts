import { SemperBenchConfig, Metric, BatchPayload } from './types';
import { performance } from 'perf_hooks';
import { randomUUID } from 'crypto';
import * as zlib from 'zlib';
import { promisify } from 'util';

const gzip = promisify(zlib.gzip);

const SDK_VERSION = '0.1.0';
const DEFAULT_ENDPOINT = 'https://ingest.semperbench.com/v1/batch';

export class SemperBenchClient {
  private config: Required<SemperBenchConfig>;
  private buffer: Metric[] = [];
  private flushTimer?: NodeJS.Timeout;
  private isFlushing = false;

  constructor(config: SemperBenchConfig) {
    this.config = {
      endpoint: DEFAULT_ENDPOINT,
      sampleRate: 0.01, // 1% default
      batchSize: 100,
      flushInterval: 60_000, // 1 minute
      debug: false,
      metadata: {},
      ...config,
    };

    if (!this.config.apiKey) {
      throw new Error('SemperBench: apiKey is required');
    }

    // Start flush timer
    this.startFlushTimer();

    // Flush on process exit
    this.setupExitHandlers();

    if (this.config.debug) {
      console.log('[SemperBench] Initialized with config:', {
        endpoint: this.config.endpoint,
        sampleRate: this.config.sampleRate,
        batchSize: this.config.batchSize,
      });
    }
  }

  /**
   * Record a metric (with sampling)
   */
  recordMetric(metric: Omit<Metric, 'timestamp' | 'traceId'>): void {
    // Sampling: only record X% of requests
    if (Math.random() > this.config.sampleRate) {
      return;
    }

    const fullMetric: Metric = {
      traceId: randomUUID(),
      timestamp: Date.now(),
      ...metric,
      metadata: {
        ...this.config.metadata,
        ...metric.metadata,
      },
    };

    this.buffer.push(fullMetric);

    if (this.config.debug) {
      console.log(`[SemperBench] Recorded metric: ${metric.endpoint} (${metric.durationMs}ms)`);
    }

    // Auto-flush if batch size reached
    if (this.buffer.length >= this.config.batchSize) {
      this.flush().catch((err) => {
        if (this.config.debug) {
          console.error('[SemperBench] Auto-flush failed:', err);
        }
      });
    }
  }

  /**
   * Create a span for fine-grained profiling
   */
  startSpan(name: string): { end: () => number } {
    const startTime = performance.now();

    return {
      end: () => {
        const duration = performance.now() - startTime;
        if (this.config.debug) {
          console.log(`[SemperBench] Span "${name}": ${duration.toFixed(2)}ms`);
        }
        return duration;
      },
    };
  }

  /**
   * Manually flush buffered metrics
   */
  async flush(): Promise<void> {
    if (this.isFlushing || this.buffer.length === 0) {
      return;
    }

    this.isFlushing = true;

    try {
      const batch = this.buffer.splice(0); // Take all buffered metrics

      const payload: BatchPayload = {
        customerId: this.config.apiKey,
        metrics: batch,
        sdkVersion: SDK_VERSION,
        batchTimestamp: Date.now(),
      };

      await this.sendBatch(payload);

      if (this.config.debug) {
        console.log(`[SemperBench] Flushed ${batch.length} metrics`);
      }
    } catch (error) {
      if (this.config.debug) {
        console.error('[SemperBench] Flush failed:', error);
      }
      // Don't throw - we don't want to crash the app
    } finally {
      this.isFlushing = false;
    }
  }

  /**
   * Send batch to ingestion endpoint
   */
  private async sendBatch(payload: BatchPayload): Promise<void> {
    const body = JSON.stringify(payload);

    // Compress with gzip
    const compressed = await gzip(body);

    const response = await fetch(this.config.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Encoding': 'gzip',
        'X-API-Key': this.config.apiKey,
        'User-Agent': `semperbench-sdk/${SDK_VERSION}`,
      },
      body: compressed,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
  }

  /**
   * Start periodic flush timer
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(() => {
      this.flush().catch((err) => {
        if (this.config.debug) {
          console.error('[SemperBench] Scheduled flush failed:', err);
        }
      });
    }, this.config.flushInterval);

    // Don't keep the process alive just for this timer
    this.flushTimer.unref();
  }

  /**
   * Setup handlers to flush on process exit
   */
  private setupExitHandlers(): void {
    const flushAndExit = async () => {
      if (this.config.debug) {
        console.log('[SemperBench] Process exiting, flushing metrics...');
      }
      await this.flush();
    };

    process.once('beforeExit', flushAndExit);
    process.once('SIGINT', flushAndExit);
    process.once('SIGTERM', flushAndExit);
  }

  /**
   * Shutdown and cleanup
   */
  async shutdown(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    await this.flush();
  }

  /**
   * Get current buffer size (for debugging)
   */
  getBufferSize(): number {
    return this.buffer.length;
  }
}
