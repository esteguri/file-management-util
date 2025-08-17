import { ChunkProcessorOptions } from "./array.processor.type";

/**
 * Splits an array into chunks of specified size
 * @param array - The array to split into chunks
 * @param batchSize - Size of each chunk (must be >= 1)
 * @returns Array of chunks
 * @throws Error if batchSize is not a finite number >= 1
 */
function toChunks<T>(array: T[], batchSize: number) {
  if (!Number.isFinite(batchSize) || batchSize < 1) {
    throw new Error(
      `"batchSize" must be an integer >= 1. Received value: ${batchSize}`
    );
  }

  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += batchSize) {
    chunks.push(array.slice(i, i + batchSize));
  }
  return chunks;
}

/**
 * Processes an array in chunks with configurable concurrency
 * @param array - The array to process
 * @param options - Processing configuration options
 * @throws Error if concurrency is not a finite number >= 1
 */
async function processInChunks<T>(
  array: T[],
  options: ChunkProcessorOptions<T>
): Promise<void> {
  const {
    batchSize,
    onChunk,
    onFinish,
    onChunkError,
    concurrency = 1,
  } = options;

  if (!Number.isFinite(concurrency) || concurrency < 1) {
    throw new Error(
      `"concurrency" must be an integer >= 1. Received value: ${concurrency}`
    );
  }

  // Create chunks of the array
  const chunks = toChunks(array, batchSize);

  // Process chunks with concurrency
  let chunkIndex = 0,
    invalidChunks = 0;

  const worker = async () => {
    while (chunkIndex < chunks.length) {
      const index = chunkIndex++;
      const chunk = chunks[index];
      try {
        await onChunk(chunk, index);
      } catch (error) {
        invalidChunks++;
        await onChunkError?.(error, chunk, index);
      } finally {
        chunks[index] = [];
      }
    }
  };

  // Launch up to "concurrency" workers (no more than the number of chunks)
  const workers = Array.from(
    { length: Math.min(concurrency, chunks.length) },
    worker
  );
  await Promise.allSettled(workers);
  await onFinish?.({
    totalChunks: chunks.length,
    failedChunks: invalidChunks,
    processedChunks: chunks.length - invalidChunks,
  });
}

/**
 * Array processing utilities for chunk-based operations
 */
export const ArrayProcessor = {
  /**
   * Processes an array in chunks with configurable concurrency
   */
  processInChunks: processInChunks,

  /**
   * Splits an array into chunks of specified size
   */
  toChunks,
};
