import { ArrayProcessor } from "./array.processor";
import { ChunkProcessorOptions } from "./array.processor.type";

describe("ArrayProcessor", () => {
  describe("toChunks", () => {
    it("should split array into chunks of specified size", () => {
      const array = [1, 2, 3, 4, 5, 6, 7, 8, 9];
      const result = ArrayProcessor.toChunks(array, 3);

      expect(result).toEqual([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
      ]);
    });

    it("should handle array not perfectly divisible by batch size", () => {
      const array = [1, 2, 3, 4, 5];
      const result = ArrayProcessor.toChunks(array, 2);

      expect(result).toEqual([[1, 2], [3, 4], [5]]);
    });

    it("should handle empty array", () => {
      const array: number[] = [];
      const result = ArrayProcessor.toChunks(array, 3);

      expect(result).toEqual([]);
    });

    it("should handle batch size larger than array length", () => {
      const array = [1, 2, 3];
      const result = ArrayProcessor.toChunks(array, 10);

      expect(result).toEqual([[1, 2, 3]]);
    });

    it("should handle batch size of 1", () => {
      const array = [1, 2, 3];
      const result = ArrayProcessor.toChunks(array, 1);

      expect(result).toEqual([[1], [2], [3]]);
    });

    it("should throw error for invalid batch size", () => {
      const array = [1, 2, 3];

      expect(() => ArrayProcessor.toChunks(array, 0)).toThrow(
        '"batchSize" must be an integer >= 1. Received value: 0'
      );

      expect(() => ArrayProcessor.toChunks(array, -1)).toThrow(
        '"batchSize" must be an integer >= 1. Received value: -1'
      );

      expect(() => ArrayProcessor.toChunks(array, NaN)).toThrow(
        '"batchSize" must be an integer >= 1. Received value: NaN'
      );

      expect(() => ArrayProcessor.toChunks(array, Infinity)).toThrow(
        '"batchSize" must be an integer >= 1. Received value: Infinity'
      );
    });

    it("should work with different data types", () => {
      const stringArray = ["a", "b", "c", "d"];
      const result = ArrayProcessor.toChunks(stringArray, 2);

      expect(result).toEqual([
        ["a", "b"],
        ["c", "d"],
      ]);
    });
  });

  describe("processInChunks", () => {
    let onChunk: jest.Mock;
    let onFinish: jest.Mock;
    let onChunkError: jest.Mock;

    beforeEach(() => {
      onChunk = jest.fn();
      onFinish = jest.fn();
      onChunkError = jest.fn();
    });

    it("should process all chunks successfully", async () => {
      const array = [1, 2, 3, 4, 5, 6];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
        onFinish,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(onChunk).toHaveBeenCalledTimes(3);
      expect(onChunk).toHaveBeenNthCalledWith(1, [1, 2], 0);
      expect(onChunk).toHaveBeenNthCalledWith(2, [3, 4], 1);
      expect(onChunk).toHaveBeenNthCalledWith(3, [5, 6], 2);

      expect(onFinish).toHaveBeenCalledWith({
        totalChunks: 3,
        processedChunks: 3,
        failedChunks: 0,
      });
    });

    it("should handle empty array", async () => {
      const array: number[] = [];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
        onFinish,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(onChunk).not.toHaveBeenCalled();
      expect(onFinish).toHaveBeenCalledWith({
        totalChunks: 0,
        processedChunks: 0,
        failedChunks: 0,
      });
    });

    it("should handle chunk processing errors", async () => {
      const array = [1, 2, 3, 4];
      onChunk.mockRejectedValueOnce(new Error("Processing failed"));

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
        onFinish,
        onChunkError,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(onChunk).toHaveBeenCalledTimes(2);
      expect(onChunkError).toHaveBeenCalledWith(expect.any(Error), [1, 2], 0);
      expect(onFinish).toHaveBeenCalledWith({
        totalChunks: 2,
        processedChunks: 1,
        failedChunks: 1,
      });
    });

    it("should process with concurrency of 1 by default", async () => {
      const array = [1, 2, 3, 4];
      const processOrder: number[] = [];

      onChunk.mockImplementation(async (chunk: number[], index: number) => {
        processOrder.push(index);
        await new Promise((resolve) => setTimeout(resolve, 10));
      });

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(processOrder).toEqual([0, 1]);
    });

    it("should process with specified concurrency", async () => {
      const array = [1, 2, 3, 4, 5, 6, 7, 8];
      const processOrder: number[] = [];

      onChunk.mockImplementation(async (chunk: number[], index: number) => {
        processOrder.push(index);
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
      });

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        concurrency: 2,
        onChunk,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(processOrder).toHaveLength(4);
      expect(processOrder).toContain(0);
      expect(processOrder).toContain(1);
      expect(processOrder).toContain(2);
      expect(processOrder).toContain(3);
    });

    it("should throw error for invalid concurrency", async () => {
      const array = [1, 2, 3];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        concurrency: 0,
        onChunk,
      };

      await expect(
        ArrayProcessor.processInChunks(array, options)
      ).rejects.toThrow(
        '"concurrency" must be an integer >= 1. Received value: 0'
      );
    });

    it("should throw error for negative concurrency", async () => {
      const array = [1, 2, 3];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        concurrency: -1,
        onChunk,
      };

      await expect(
        ArrayProcessor.processInChunks(array, options)
      ).rejects.toThrow(
        '"concurrency" must be an integer >= 1. Received value: -1'
      );
    });

    it("should throw error for NaN concurrency", async () => {
      const array = [1, 2, 3];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        concurrency: NaN,
        onChunk,
      };

      await expect(
        ArrayProcessor.processInChunks(array, options)
      ).rejects.toThrow(
        '"concurrency" must be an integer >= 1. Received value: NaN'
      );
    });

    it("should handle synchronous onChunk callbacks", async () => {
      const array = [1, 2, 3, 4];
      const processedChunks: number[][] = [];

      onChunk.mockImplementation((chunk: number[]) => {
        processedChunks.push([...chunk]);
      });

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
        onFinish,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(processedChunks).toEqual([
        [1, 2],
        [3, 4],
      ]);
      expect(onFinish).toHaveBeenCalledWith({
        totalChunks: 2,
        processedChunks: 2,
        failedChunks: 0,
      });
    });

    it("should limit concurrency to number of chunks", async () => {
      const array = [1, 2];
      const processOrder: number[] = [];

      onChunk.mockImplementation(async (chunk: number[], index: number) => {
        processOrder.push(index);
      });

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        concurrency: 10, // Much higher than chunks available
        onChunk,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(processOrder).toEqual([0]);
      expect(onChunk).toHaveBeenCalledTimes(1);
    });

    it("should handle mixed success and failure chunks", async () => {
      const array = [1, 2, 3, 4, 5, 6];

      onChunk
        .mockResolvedValueOnce(undefined) // Success
        .mockRejectedValueOnce(new Error("Chunk 1 failed")) // Failure
        .mockResolvedValueOnce(undefined); // Success

      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
        onFinish,
        onChunkError,
      };

      await ArrayProcessor.processInChunks(array, options);

      expect(onChunk).toHaveBeenCalledTimes(3);
      expect(onChunkError).toHaveBeenCalledTimes(1);
      expect(onFinish).toHaveBeenCalledWith({
        totalChunks: 3,
        processedChunks: 2,
        failedChunks: 1,
      });
    });

    it("should work without optional callbacks", async () => {
      const array = [1, 2, 3, 4];
      const options: ChunkProcessorOptions<number> = {
        batchSize: 2,
        onChunk,
      };

      await expect(
        ArrayProcessor.processInChunks(array, options)
      ).resolves.toBeUndefined();

      expect(onChunk).toHaveBeenCalledTimes(2);
    });
  });
});
