import { ReadStream } from "fs";
import { Readable } from "stream";

/**
 * Configuration options for basic file processing
 */
export type FileProcessorOptions = {
  /**
   * Character encoding for reading the file
   * @default "utf8"
   */
  encoding?: BufferEncoding;

  /**
   * Indicates if the file has a header line that should be processed separately
   * @default false
   */
  hasHeader?: boolean;

  /**
   * Callback executed when processing the file header (only if hasHeader is true)
   * @param header - The header line of the file
   */
  onHeader?: (header: string) => void | Promise<void>;

  /**
   * Callback executed for each processed row of the file
   * @param row - The row content as string
   * @param index - The row index (starting from 0, excluding header if exists)
   */
  onRow?: (row: string, index: number) => void | Promise<void>;

  /**
   * Callback executed when finished processing the entire file
   * @param totalRows - The total number of processed rows (excluding header)
   */
  onFinish?: (totalRows: number) => void | Promise<void>;

  /**
   * Custom validation function for each row
   * @param row - The row content to validate
   * @param index - The row index
   * @returns true if the row is valid, false otherwise
   */
  onValidate?: (row: string, index: number) => boolean | Promise<boolean>;

  /**
   * If true, invalid rows will be skipped instead of throwing an error
   * @default false
   */
  skipInvalid?: boolean;

  /**
   * Callback executed when a row fails validation
   * @param row - The invalid row content
   * @param index - The invalid row index
   * @param error - Optional error associated with the validation
   */
  onDataInvalid?: (
    row: string,
    index: number,
    error?: any
  ) => void | Promise<void>;

  /**
   * Callback executed when an unexpected error occurs during processing
   * @param error - The error that occurred
   */
  onError?: (error: any) => void | Promise<void>;
};

/**
 * Extended configuration options for chunk/batch file processing
 * Inherits all properties from FileProcessorOptions
 */
export type FileProcessorChunkOptions = FileProcessorOptions & {
  /**
   * Number of rows that make up each chunk/batch
   * Must be greater than 0
   */
  batchSize: number;

  /**
   * Callback executed for each chunk/batch of processed rows
   * @param chunk - Array of strings containing the chunk rows
   * @param chunkIndex - Chunk index (starting from 0)
   */
  onChunk: (chunk: string[], chunkIndex: number) => void | Promise<void>;

  /**
   * Callback executed when an error occurs while processing a specific chunk
   * Only executed if stopOnChunkError is false
   * @param error - The error that occurred during chunk processing
   * @param chunk - The chunk being processed when the error occurred
   */
  onChunkError?: (error: any, chunk: string[]) => void | Promise<void>;

  /**
   * If true, processing stops when an error occurs in a chunk
   * If false, continues processing subsequent chunks
   * @default true
   */
  stopOnChunkError?: boolean;
};

/**
 * Input types supported by the file processor
 * Includes Node.js streams, Web API ReadableStream, and Blob
 */
export type StreamInput = ReadStream | ReadableStream | Readable | Blob;
