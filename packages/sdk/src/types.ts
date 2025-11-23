/**
 * Core types for SemperBench SDK
 */

export interface SemperBenchConfig {
  /** API key for authentication */
  apiKey: string;

  /** Ingestion endpoint URL */
  endpoint?: string;

  /** Sampling rate (0.0 - 1.0). Default: 0.01 (1%) */
  sampleRate?: number;

  /** Max batch size before auto-flush. Default: 100 */
  batchSize?: number;

  /** Flush interval in milliseconds. Default: 60000 (1 min) */
  flushInterval?: number;

  /** Enable debug logging */
  debug?: boolean;

  /** Custom metadata to attach to all metrics */
  metadata?: Record<string, string>;
}

export interface Metric {
  /** Unique trace/request ID */
  traceId: string;

  /** Timestamp (Unix epoch milliseconds) */
  timestamp: number;

  /** HTTP method */
  method?: string;

  /** Endpoint/route */
  endpoint: string;

  /** Duration in milliseconds */
  durationMs: number;

  /** Memory usage in MB */
  memoryMb?: number;

  /** CPU usage percentage (0-100) */
  cpuPercent?: number;

  /** HTTP status code */
  statusCode?: number;

  /** Error message if failed */
  error?: string;

  /** Custom metadata */
  metadata?: Record<string, any>;
}

export interface Span {
  /** Span name (e.g., "db.query", "external.api") */
  name: string;

  /** Start time (high-resolution) */
  startTime: number;

  /** End time (high-resolution) */
  endTime?: number;

  /** Parent trace ID */
  traceId: string;

  /** Attributes */
  attributes?: Record<string, any>;
}

export interface BatchPayload {
  /** Customer/API key identifier */
  customerId: string;

  /** Batch of metrics */
  metrics: Metric[];

  /** SDK version */
  sdkVersion: string;

  /** Batch timestamp */
  batchTimestamp: number;
}
