import { ReadStream } from "fs";
import { Readable } from "stream";
import {
  FileProcessorChunkOptions,
  FileProcessorOptions,
  StreamInput,
} from "./file.processor.type";

class FileProcessorClass {
  private static readonly DEFAULT_ENCODING: BufferEncoding = "utf8";
  private static readonly LINE_BREAK_REGEX = /\r\n|\r|\n/;

  /**
   * Reads the entire file efficiently line by line
   */
  public static async read(
    input: StreamInput,
    options: FileProcessorOptions
  ): Promise<void> {
    const stream = this.createStream(input);
    const { hasHeader = false, skipInvalid = false } = options;

    let buffer = "",
      rowIndex = 0,
      isFirstRow = true,
      totalRows = 0;

    return new Promise((resolve, reject) => {
      stream.on("data", async (chunk: Buffer | string) => {
        try {
          buffer += chunk.toString(options.encoding || this.DEFAULT_ENCODING);
          const lines = buffer.split(this.LINE_BREAK_REGEX);

          // Keep the last incomplete line in the buffer
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.trim() === "") continue;

            // Process header if configured
            if (hasHeader && isFirstRow) {
              await options.onHeader?.(line);
              isFirstRow = false;
              continue;
            }

            // Validate row if validation callback exists
            if (options.onValidate) {
              const isValid = await options.onValidate(line, rowIndex);
              if (!isValid) {
                await options.onDataInvalid?.(line, rowIndex);
                if (!skipInvalid) {
                  throw new Error(`Invalid data at row ${rowIndex}: ${line}`);
                }
                rowIndex++;
                continue;
              }
            }

            await options.onRow?.(line, rowIndex);

            rowIndex++;
            totalRows++;
            isFirstRow = false;
          }
        } catch (error) {
          reject(error);
        }
      });

      stream.on("end", async () => {
        try {
          // Process last line if it exists
          if (buffer.trim()) {
            if (options.onValidate) {
              const isValid = await options.onValidate(buffer, rowIndex);
              if (!isValid) {
                await options.onDataInvalid?.(buffer, rowIndex);
                if (!skipInvalid) {
                  throw new Error(`Invalid data at row ${rowIndex}: ${buffer}`);
                }
              } else {
                await options.onRow?.(buffer, rowIndex);
                totalRows++;
              }
            } else {
              await options.onRow?.(buffer, rowIndex);
              totalRows++;
            }
          }

          await options.onFinish?.(totalRows);

          resolve();
        } catch (error) {
          options.onError?.(error);
          reject(error);
        }
      });

      stream.on("error", (error) => reject(error));
    });
  }

  /**
   * Reads the file in chunks/batches of specific size
   */
  public static async readByChunks(
    input: StreamInput,
    options: FileProcessorChunkOptions
  ): Promise<void> {
    if (options.batchSize <= 0) {
      throw new Error("batchSize must be greater than 0");
    }

    const stream = this.createStream(input);
    const {
      hasHeader = false,
      skipInvalid = false,
      stopOnChunkError = true,
    } = options;

    let buffer = "",
      rowIndex = 0,
      chunkIndex = 1,
      currentChunk: string[] = [],
      isFirstRow = true,
      totalRows = 0,
      failedChunks = 0;

    return new Promise((resolve, reject) => {
      stream.on("data", async (chunk: Buffer | string) => {
        try {
          buffer += chunk.toString(options.encoding || this.DEFAULT_ENCODING);
          const lines = buffer.split(this.LINE_BREAK_REGEX);

          // Keep the last incomplete line in the buffer
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.trim() === "") continue;

            // Process header if configured
            if (hasHeader && isFirstRow) {
              await options.onHeader?.(line);
              isFirstRow = false;
              continue;
            }

            // Validate row if validation callback exists
            if (options.onValidate) {
              const isValid = await options.onValidate(line, rowIndex);
              if (!isValid) {
                await options.onDataInvalid?.(line, rowIndex);
                if (!skipInvalid) {
                  throw new Error(`Invalid data at row ${rowIndex}: ${line}`);
                }
                rowIndex++;
                continue;
              }
            }

            await options.onRow?.(line, rowIndex);

            currentChunk.push(line);
            rowIndex++;
            totalRows++;
            isFirstRow = false;

            // Process chunk when it reaches the desired size
            if (currentChunk.length >= options.batchSize) {
              await this.runOnChunk(currentChunk, chunkIndex, options)
                .catch((error) => {
                  failedChunks++;
                  throw error;
                })
                .finally(() => {
                  currentChunk = [];
                  chunkIndex++;
                });
            }
          }
        } catch (error) {
          reject(error);
        }
      });

      stream.on("end", async () => {
        try {
          const stopProcessing = stopOnChunkError && failedChunks > 0;

          // Process last line if it exists
          if (!stopProcessing && buffer.trim()) {
            if (options.onValidate) {
              const isValid = await options.onValidate(buffer, rowIndex);
              if (!isValid) {
                if (options.onDataInvalid) {
                  await options.onDataInvalid(buffer, rowIndex);
                }
                if (!skipInvalid) {
                  throw new Error(`Invalid data at row ${rowIndex}: ${buffer}`);
                }
              } else {
                await options.onRow?.(buffer, rowIndex);
                currentChunk.push(buffer);
                totalRows++;
              }
            } else {
              await options.onRow?.(buffer, rowIndex);
              currentChunk.push(buffer);
              totalRows++;
            }
          }

          // Process final chunk if it contains data
          if (!stopProcessing && currentChunk.length > 0) {
            console.log("ultimo chunk", currentChunk);
            await this.runOnChunk(currentChunk, chunkIndex, options);
          }

          await options.onFinish?.(totalRows);

          resolve();
        } catch (error) {
          options.onError?.(error);
          reject(error);
        }
      });

      stream.on("error", (error) => reject(error));
    });
  }

  private static async runOnChunk(
    chunk: string[],
    chunkIndex: number,
    {
      onChunk,
      onChunkError,
      stopOnChunkError = true,
    }: FileProcessorChunkOptions
  ) {
    try {
      await onChunk(chunk, chunkIndex);
    } catch (error) {
      if (stopOnChunkError) throw error;

      if (onChunkError) {
        await onChunkError(error, chunk);
      } else {
        console.warn(
          `Warning: Error processing chunk ${chunkIndex}. Process continues since stopOnChunkError is false. Consider using onChunkError callback for better error handling. Exception: ${error}`
        );
      }
    }
  }

  /**
   * Creates an appropriate Node.js Readable stream from various input types (ReadableStream, ReadStream, or Readable)
   */
  private static createStream(input: StreamInput): Readable {
    if (input instanceof ReadableStream) {
      // Web ReadableStream - convert to Node.js Readable
      return Readable.fromWeb(input as any);
    } else if (input instanceof ReadStream) {
      // Node.js ReadStream
      return input;
    } else if (input instanceof Readable) {
      // Node.js Readable stream
      return input;
    } else {
      throw new Error(
        "Unsupported input type. Expected string, ReadStream, Readable, ReadableStream or object with transformToWebStream method."
      );
    }
  }
}

export const FileProcessor = {
  read: FileProcessorClass.read.bind(FileProcessorClass),
  readByChunks: FileProcessorClass.readByChunks.bind(FileProcessorClass),
};
