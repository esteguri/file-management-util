import { ReadStream } from "fs";
import { Readable } from "stream";

export type FileProcessorCommonOptions = {
  /**
   * File character encoding
   * @default "utf8"
   */
  encoding?: BufferEncoding;

  /**
   * Whether file has header line
   * @default false
   */
  hasHeader?: boolean;

  /**
   * Header processing callback
   * @param header - Header line content
   */
  onHeader?: (header: string) => void | Promise<void>;

  /**
   * Row processing callback
   * @param row - Row content
   * @param index - Row index (0-based, excludes header)
   */
  onRow?: (row: string, index: number) => void | Promise<void>;

  /**
   * Row validation function
   * @param row - Row content
   * @param index - Row index
   * @returns Validation result
   */
  onValidate?: (row: string, index: number) => boolean | Promise<boolean>;

  /**
   * Skip invalid rows instead of throwing error
   * @default false
   */
  skipInvalid?: boolean;

  /**
   * Invalid row callback
   * @param row - Invalid row content
   * @param index - Row index
   * @param error - Validation error
   */
  onDataInvalid?: (
    row: string,
    index: number,
    error?: any
  ) => void | Promise<void>;

  /**
   * Error handling callback
   * @param error - Processing error
   */
  onError?: (error: any) => void | Promise<void>;
};

/**
 * Basic file processing options
 */
export type FileProcessorOptions = FileProcessorCommonOptions & {
  /**
   * Processing completion callback
   * @param summary - Processing summary
   */
  onFinish?: (summary: ProcessingSummary) => void | Promise<void>;
};

/**
 * Chunk-based file processing options
 */
export type FileProcessorChunkOptions = FileProcessorCommonOptions & {
  /**
   * Rows per chunk (must be > 0)
   */
  batchSize: number;

  /**
   * Chunk processing callback
   * @param chunk - Chunk rows
   * @param chunkIndex - Chunk index (0-based)
   */
  onChunk: (chunk: string[], chunkIndex: number) => void | Promise<void>;

  /**
   * Chunk error callback
   * @param error - Chunk processing error
   * @param chunk - Failed chunk
   */
  onChunkError?: (error: any, chunk: string[]) => void | Promise<void>;

  /**
   * Stop processing on chunk error
   * @default false
   */
  stopOnChunkError?: boolean;

  /**
   * Processing completion callback
   * @param summary - Processing summary
   */
  onFinish?: (summary: ProcessingChunkSummary) => void | Promise<void>;
};

/**
 * File processing summary
 */
export type ProcessingSummary = {
  /** Total rows processed */
  totalRows: number;
  /** Successfully processed rows */
  processedRows: number;
  /** Invalid rows count */
  invalidRows: number;
};

/**
 * Chunk processing summary
 */
export type ProcessingChunkSummary = ProcessingSummary & {
  /** Total chunks processed */
  totalChunks: number;
  /** Failed chunks count */
  failedChunks: number;
};

/**
 * Parameters for chunk processing function
 * @param chunk - Array of string rows in the current chunk
 * @param chunkIndex - Index of current chunk being processed
 * @param options - Chunk processing configuration options
 * @param onError - Error handler callback function
 */
export type RunOnChunkParams = {
  chunk: string[];
  chunkIndex: number;
  options: FileProcessorChunkOptions;
  onError: () => void;
};

/**
 * Supported stream input types
 */
export type StreamInput = ReadStream | ReadableStream | Readable | Blob;
