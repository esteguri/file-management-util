import * as FastCsv from "fast-csv";
import { Readable } from "stream";
import { pipeline } from "stream/promises";

type CsvRow = {
  [key: string]: string;
};

type CsvProcessOptions<T> = FastCsv.ParserOptionsArgs & {
  skipInvalidRows?: boolean;
  onHeaders?: (headers: string[]) => void;
  onFinish?: () => Promise<void>;
  onValidate?: (row: T) => boolean;
  onDataInvalid?: (row: T, rowNumber: number) => void;
  onRow?: (row: T) => Promise<void>;
};

type CsvProcessStreamOptions<T> = CsvProcessOptions<T> & {
  batchSize: number;
  onChunk: (rows: T[]) => Promise<void>;
};

type BuildParserParams<T> = {
  options: CsvProcessOptions<T>;
  onDataEvent: (row: T) => Promise<void>;
  onEndEvent: () => Promise<void>;
};

const processByChunks = async (
  stream: string | Buffer | Readable | ReadableStream,
  options: CsvProcessStreamOptions<CsvRow>
) => {
  const rows: CsvRow[] = [];

  const parser = buildParser<CsvRow>({
    options,
    onDataEvent: async (row) => {
      rows.push(row);
      await options.onRow?.(row);
      if (rows.length >= options.batchSize) {
        await options.onChunk(rows.splice(0, options.batchSize));
      }
    },
    onEndEvent: async () => {
      await options.onFinish?.();
      if (rows.length > 0) {
        await options.onChunk(rows);
      }
    },
  });

  await pipeline(stream, parser);
};

const process = async (
  stream: string | Buffer | Readable | ReadableStream,
  options: CsvProcessOptions<CsvRow>
) => {
  const parser = buildParser<CsvRow>({
    options,
    onDataEvent: async (row) => {
      await options.onRow?.(row);
    },
    onEndEvent: async () => {
      await options.onFinish?.();
    },
  });

  await pipeline(stream, parser);
};

const buildParser = <T>({
  options,
  onDataEvent,
  onEndEvent,
}: BuildParserParams<T>) => {
  return FastCsv.parse(options)
    .validate((row: object) => options.onValidate?.(row as T) ?? true)
    .on("headers", (headers) => {
      options.onHeaders?.(headers);
    })
    .on("data", onDataEvent)
    .on("data-invalid", (row: T, rowNumber: number) => {
      console.log(
        `Invalid [rowNumber=${rowNumber}] [row=${JSON.stringify(row)}]`
      );

      options.onDataInvalid?.(row, rowNumber);

      if (!Boolean(options.skipInvalidRows ?? false)) {
        throw new Error(`Invalid row [rowNumber=${rowNumber}]`);
      }
    })
    .on("end", onEndEvent)
    .on("error", (error) => {
      console.error("Ocurrio un error procesando el archivo", error.message);
    });
};

export const CsvProcessor = {
  processByChunks,
  process,
};
