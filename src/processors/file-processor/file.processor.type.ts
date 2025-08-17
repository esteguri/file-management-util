import { ReadStream } from "fs";
import { Readable } from "stream";

export type FileProcessorOptions = FileProcessorCallbacks & {
  hasHeader?: boolean;
  skipInvalid?: boolean;
  encoding?: BufferEncoding;
};

export type FileProcessorCallbacks = {
  onHeader?: (header: string) => void | Promise<void>;
  onRow?: (row: string, index: number) => void | Promise<void>;
  onFinish?: (totalRows: number) => void | Promise<void>;
  onValidate?: (row: string, index: number) => boolean | Promise<boolean>;
  onDataInvalid?: (
    row: string,
    index: number,
    error?: any
  ) => void | Promise<void>;
  onError?: (error: any) => void | Promise<void>;
};

export type FileProcessorChunkOptions = FileProcessorOptions & {
  batchSize: number;
  stopOnChunkError?: boolean;
  onChunk: (chunk: string[], chunkIndex: number) => void | Promise<void>;
  onChunkError?: (error: any, chunk: string[]) => void | Promise<void>;
};

export type StreamInput = ReadStream | ReadableStream | Readable | Blob;
