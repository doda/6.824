/**
 * SemperBench SDK - Continuous performance profiling for Node.js
 *
 * @example
 * ```typescript
 * import { SemperBench } from '@semperbench/sdk';
 *
 * const semperbench = new SemperBench({
 *   apiKey: 'your-api-key',
 *   sampleRate: 0.05 // Profile 5% of requests
 * });
 *
 * // Express middleware
 * app.use(semperbench.middleware());
 * ```
 */

export { SemperBenchClient } from './client';
export { semperBenchMiddleware, profile, getMemoryStats } from './middleware';
export type { SemperBenchConfig, Metric, Span, BatchPayload } from './types';

import { SemperBenchClient } from './client';
import { semperBenchMiddleware } from './middleware';
import { SemperBenchConfig } from './types';

/**
 * Main SemperBench class (convenience wrapper)
 */
export class SemperBench extends SemperBenchClient {
  /**
   * Get Express middleware
   */
  middleware() {
    return semperBenchMiddleware(this);
  }
}

/**
 * Default export
 */
export default SemperBench;
