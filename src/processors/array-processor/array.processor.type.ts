/**
 * Configuration options for chunk processing
 */
export type ChunkProcessorOptions<T> = {
  /**
   * Size of each chunk to process
   */
  batchSize: number;

  /**
   * Maximum number of chunks to process concurrently
   * @default 1
   */
  concurrency?: number;

  /**
   * Callback executed for each chunk
   * @param chunk - Array of items in the chunk
   * @param index - Zero-based chunk index
   */
  onChunk: (chunk: T[], index: number) => Promise<void> | void;

  /**
   * Callback executed when all chunks are processed
   * @param summary - Processing results summary
   */
  onFinish?: (summary: ProcessingSummary) => Promise<void> | void;

  /**
   * Callback executed when a chunk processing fails
   * @param error - The error that occurred
   * @param chunk - The chunk that failed to process
   * @param index - Zero-based chunk index
   */
  onChunkError?: (
    error: unknown,
    chunk: T[],
    index: number
  ) => Promise<void> | void;
};

/**
 * Summary of chunk processing results
 */
export type ProcessingSummary = {
  /** Total number of chunks created */
  totalChunks: number;

  /** Number of chunks processed successfully */
  processedChunks: number;

  /** Number of chunks that failed to process */
  failedChunks: number;
};
