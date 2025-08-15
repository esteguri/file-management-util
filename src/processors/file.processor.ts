import { ReadStream } from "fs";
import { Readable } from "stream";

// Tipos para los callbacks y configuraciones
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
};

export type FileProcessorChunkOptions = FileProcessorOptions & {
  batchSize: number;
  onChunk: (chunk: string[], chunkIndex: number) => void | Promise<void>;
};

export type StreamInput = ReadStream | ReadableStream | Readable | string;

class FileProcessorClass {
  private static readonly DEFAULT_ENCODING: BufferEncoding = "utf8";
  private static readonly LINE_BREAK_REGEX = /\r\n|\r|\n/;

  /**
   * Lee todo el archivo de manera eficiente línea por línea
   */
  public static async read(
    input: StreamInput,
    options: FileProcessorOptions
  ): Promise<void> {
    const stream = this.createStream(input);
    const { hasHeader = false, skipInvalid = false } = options;

    let buffer = "";
    let rowIndex = 0;
    let isFirstRow = true;
    let totalRows = 0;

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
          reject(error);
        }
      });

      stream.on("error", (error) => reject(error));
    });
  }

  /**
   * Lee el archivo por chunks/lotes de tamaño específico
   */
  public static async readByChunks(
    input: StreamInput,
    options: FileProcessorChunkOptions
  ): Promise<void> {
    if (options.batchSize <= 0) {
      throw new Error("batchSize must be greater than 0");
    }

    const stream = this.createStream(input);
    const { hasHeader = false, skipInvalid = false } = options;

    let buffer = "";
    let rowIndex = 0;
    let chunkIndex = 0;
    let currentChunk: string[] = [];
    let isFirstRow = true;
    let totalRows = 0;

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

            // Procesar chunk cuando alcance el tamaño deseado
            if (currentChunk.length >= options.batchSize) {
              await options.onChunk([...currentChunk], chunkIndex);
              currentChunk = [];
              chunkIndex++;
            }
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
          if (currentChunk.length > 0) {
            await options.onChunk([...currentChunk], chunkIndex);
          }

          await options.onFinish?.(totalRows);

          resolve();
        } catch (error) {
          reject(error);
        }
      });

      stream.on("error", (error) => {
        reject(error);
      });
    });
  }

  /**
   * Creates an appropriate stream based on the input type
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
        "Unsupported input type. Expected string, ReadStream, Readable, or ReadableStream."
      );
    }
  }
}

// Funciones de conveniencia para uso directo

export const FileProcessor = {
  read: FileProcessorClass.read.bind(FileProcessorClass),
  readByChunks: FileProcessorClass.readByChunks.bind(FileProcessorClass),
};
