import { SemperBenchClient } from './client';
import { Request, Response, NextFunction } from 'express';
import { performance } from 'perf_hooks';
import * as v8 from 'v8';

/**
 * Express middleware for automatic request profiling
 */
export function semperBenchMiddleware(client: SemperBenchClient) {
  return (req: Request, res: Response, next: NextFunction) => {
    const startTime = performance.now();
    const startMemory = process.memoryUsage().heapUsed;
    const startCpu = process.cpuUsage();

    // Capture response
    const originalSend = res.send;
    res.send = function (data: any) {
      const duration = performance.now() - startTime;
      const endMemory = process.memoryUsage().heapUsed;
      const endCpu = process.cpuUsage(startCpu);

      // Calculate CPU percentage (rough estimate)
      const cpuUsage = (endCpu.user + endCpu.system) / 1000; // microseconds -> milliseconds
      const cpuPercent = (cpuUsage / duration) * 100;

      // Record metric
      client.recordMetric({
        endpoint: req.route?.path || req.path,
        method: req.method,
        durationMs: duration,
        memoryMb: (endMemory - startMemory) / 1024 / 1024,
        cpuPercent: Math.min(cpuPercent, 100), // Cap at 100%
        statusCode: res.statusCode,
        metadata: {
          userAgent: req.get('user-agent') || '',
          ip: req.ip || '',
        },
      });

      return originalSend.call(this, data);
    };

    next();
  };
}

/**
 * Get memory usage statistics
 */
export function getMemoryStats() {
  const usage = process.memoryUsage();
  const heapStats = v8.getHeapStatistics();

  return {
    heapUsedMb: usage.heapUsed / 1024 / 1024,
    heapTotalMb: usage.heapTotal / 1024 / 1024,
    rssMb: usage.rss / 1024 / 1024,
    externalMb: usage.external / 1024 / 1024,
    heapSizeLimitMb: heapStats.heap_size_limit / 1024 / 1024,
  };
}

/**
 * Manual profiling wrapper
 */
export async function profile<T>(
  client: SemperBenchClient,
  name: string,
  fn: () => Promise<T>
): Promise<T> {
  const span = client.startSpan(name);

  try {
    const result = await fn();
    span.end();
    return result;
  } catch (error) {
    span.end();
    throw error;
  }
}
