import { FileProcessor } from "./file.processor";

function toStream(content: string[], delimiter: string = "\n"): ReadableStream {
  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(content.join(delimiter));
      controller.close();
    },
  });

  return stream;
}

describe("FileProcessor.read", () => {
  it("should process all lines and call onRow for each", async () => {
    const input = toStream(["a", "b", "c"]);
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });

  it("should support header and call onHeader once", async () => {
    const input = toStream(["HEADER", "row1", "row2"]);
    const onHeader = jest.fn();
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, {
      hasHeader: true,
      onHeader,
      onRow,
      onFinish,
    });

    expect(onHeader).toHaveBeenCalledTimes(1);
    expect(onHeader).toHaveBeenCalledWith("HEADER");

    expect(onRow).toHaveBeenCalledTimes(2);
    expect(onRow).toHaveBeenNthCalledWith(1, "row1", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "row2", 1);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 2,
      invalidRows: 0,
      processedRows: 2,
    });
  });

  it("should process last line even without trailing newline", async () => {
    const input = toStream(["a", "b", "c"]);
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });

  it("should reject when validation fails and skipInvalid is false", async () => {
    const input = toStream(["a", "invalid", "b"]);
    const onValidate = jest.fn(async (row: string) => row !== "invalid");
    const onDataInvalid = jest.fn();

    await expect(
      FileProcessor.read(input, {
        onValidate,
        onDataInvalid,
        skipInvalid: false,
      })
    ).rejects.toThrow("Invalid data at row 1: invalid");

    expect(onValidate).toHaveBeenCalledTimes(2);
    expect(onDataInvalid).toHaveBeenCalledTimes(1);
    expect(onDataInvalid).toHaveBeenCalledWith("invalid", 1);
  });

  it("should skip invalid rows when skipInvalid is true", async () => {
    const input = toStream(["a", "invalid", "b"]);
    const onValidate = jest.fn(async (row: string) => row !== "invalid");
    const onRow = jest.fn();
    const onDataInvalid = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, {
      onValidate,
      onRow,
      onDataInvalid,
      skipInvalid: true,
      onFinish,
    });

    expect(onRow).toHaveBeenCalledTimes(2);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 2);

    expect(onDataInvalid).toHaveBeenCalledTimes(1);
    expect(onDataInvalid).toHaveBeenCalledWith("invalid", 1);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 1,
      processedRows: 2,
    });
  });
});

describe("FileProcessor.readInChunks", () => {
  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("should throw if batchSize is <= 0", async () => {
    const input = toStream(["a"]);
    await expect(
      FileProcessor.readInChunks(input, {
        batchSize: 0,
        onChunk: jest.fn(),
      })
    ).rejects.toThrow("batchSize must be greater than 0");
  });

  it("should process chunks of given size and call onChunk with proper index", async () => {
    const input = toStream(["a", "b", "c", "d"]);
    const onRow = jest.fn();
    const onChunk = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.readInChunks(input, {
      batchSize: 2,
      onRow,
      onChunk,
      onFinish,
    });

    expect(onRow).toHaveBeenCalledTimes(4);

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, ["a", "b"], 1);
    expect(onChunk).toHaveBeenNthCalledWith(2, ["c", "d"], 2);

    // We assert the rows metrics and failures; totalChunks is implementation-defined here
    expect(onFinish).toHaveBeenCalledWith(
      expect.objectContaining({
        totalRows: 4,
        invalidRows: 0,
        processedRows: 4,
        failedChunks: 0,
      })
    );
  });

  it("should process remaining lines as final smaller chunk", async () => {
    const input = toStream(["a", "b", "c"]);
    const onChunk = jest.fn();

    await FileProcessor.readInChunks(input, {
      batchSize: 2,
      onChunk,
    });

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, ["a", "b"], 1);
    expect(onChunk).toHaveBeenNthCalledWith(2, ["c"], 2);
  });

  it("should stop on chunk error when stopOnChunkError is true", async () => {
    const input = toStream(["a", "b", "c", "d"]);
    const error = new Error("chunk failure");
    const onChunk = jest.fn().mockImplementationOnce(() => {
      throw error;
    });

    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    await expect(
      FileProcessor.readInChunks(input, {
        batchSize: 2,
        onChunk,
        stopOnChunkError: true,
      })
    ).rejects.toThrow("chunk failure");

    // onChunkError is not called when stopOnChunkError is true
    expect(warnSpy).not.toHaveBeenCalled();
  });

  it("should continue and call onChunkError when stopOnChunkError is false", async () => {
    const input = toStream(["a", "b", "c", "d"]);
    const error = new Error("chunk failure");

    const onChunk = jest
      .fn()
      .mockImplementationOnce(() => {
        throw error;
      })
      .mockResolvedValueOnce(undefined); // second chunk success

    const onChunkError = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.readInChunks(input, {
      batchSize: 2,
      onChunk,
      onChunkError,
      stopOnChunkError: false,
      onFinish,
    });

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunkError).toHaveBeenCalledTimes(1);
    // onChunkError receives (error, chunk)
    expect(onChunkError).toHaveBeenCalledWith(error, ["a", "b"]);

    expect(onFinish).toHaveBeenCalledWith(
      expect.objectContaining({
        totalRows: 4,
        invalidRows: 0,
        processedRows: 4,
        failedChunks: 1,
      })
    );
  });

  it("should warn when chunk fails and onChunkError is not provided", async () => {
    const input = toStream(["a", "b"]);
    const error = new Error("oops");
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    const onChunk = jest.fn().mockImplementationOnce(() => {
      throw error;
    });

    await FileProcessor.readInChunks(input, {
      batchSize: 2,
      onChunk,
      stopOnChunkError: false,
    });

    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(warnSpy.mock.calls[0][0]).toMatch(
      /Warning: Error processing chunk 1/
    );
  });

  it("should reject when a row is invalid and skipInvalid is false (before chunk)", async () => {
    const input = toStream(["ok", "bad", "ok2"]);
    const onValidate = jest.fn(async (row: string) => row !== "bad");
    const onDataInvalid = jest.fn();

    await expect(
      FileProcessor.readInChunks(input, {
        batchSize: 2,
        onChunk: jest.fn(),
        onValidate,
        onDataInvalid,
        skipInvalid: false,
      })
    ).rejects.toThrow("Invalid data at row 1: bad");

    expect(onDataInvalid).toHaveBeenCalledWith("bad", 1);
  });

  it("should skip invalid rows when skipInvalid is true and still form chunks", async () => {
    const input = toStream(["a", "bad", "b", "c"]);
    const onValidate = jest.fn(async (row: string) => row !== "bad");
    const onRow = jest.fn();
    const onChunk = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.readInChunks(input, {
      batchSize: 2,
      onValidate,
      onRow,
      onChunk,
      onFinish,
      skipInvalid: true,
    });

    // Rows processed (onRow called for valid rows only)
    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 2);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 3);

    // Chunks should be ['a','b'] and ['c']
    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, ["a", "b"], 1);
    expect(onChunk).toHaveBeenNthCalledWith(2, ["c"], 2);

    expect(onFinish).toHaveBeenCalledWith(
      expect.objectContaining({
        totalRows: 4,
        invalidRows: 1,
        processedRows: 3,
        failedChunks: 0,
      })
    );
  });

  it("should support header in chunked mode", async () => {
    const input = toStream(["HEADER", "r1", "r2", "r3"]);
    const onHeader = jest.fn();
    const onRow = jest.fn();
    const onChunk = jest.fn();

    await FileProcessor.readInChunks(input, {
      hasHeader: true,
      batchSize: 2,
      onHeader,
      onRow,
      onChunk,
    });

    expect(onHeader).toHaveBeenCalledWith("HEADER");

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "r1", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "r2", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "r3", 2);

    expect(onChunk).toHaveBeenCalledTimes(2);
    expect(onChunk).toHaveBeenNthCalledWith(1, ["r1", "r2"], 1);
    expect(onChunk).toHaveBeenNthCalledWith(2, ["r3"], 2);
  });
});

describe("FileProcessor line delimiter support", () => {
  it("should support CRLF (\\r\\n) line endings", async () => {
    const input = toStream(["a", "b", "c"], "\r\n");
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });

  it("should support CR (\\r) line endings", async () => {
    const input = toStream(["a", "b", "c"], "\r");
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });

  it("should support LF (\\n) line endings", async () => {
    const input = toStream(["a", "b", "c"], "\n");
    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(input, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });

  it("should handle mixed line endings in the same file", async () => {
    // Create a stream directly with different delimiters
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue("a\r\nb\rc\n");
        controller.close();
      },
    });

    const onRow = jest.fn();
    const onFinish = jest.fn();

    await FileProcessor.read(stream, { onRow, onFinish });

    expect(onRow).toHaveBeenCalledTimes(3);
    expect(onRow).toHaveBeenNthCalledWith(1, "a", 0);
    expect(onRow).toHaveBeenNthCalledWith(2, "b", 1);
    expect(onRow).toHaveBeenNthCalledWith(3, "c", 2);

    expect(onFinish).toHaveBeenCalledWith({
      totalRows: 3,
      invalidRows: 0,
      processedRows: 3,
    });
  });
});
