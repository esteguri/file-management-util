import { FileProcessor } from "./processors/file-processor/file.processor";
import { s3Service } from "./services/s3-service";

console.log("¡Hola desde TypeScript!");
console.log("Bucket name:", process.env.BUCKET_NAME);
console.log("Localstack endpoint:", process.env.LOCALSTACK_ENDPOINT);

/**
 * Función para cargar un archivo CSV a S3
 * @param filePath - Ruta local del archivo CSV
 * @param s3Key - Nombre/ruta del archivo en S3
 * @param contentType - Tipo de contenido del archivo
 * @param metadata - Metadatos del archivo (opcional)
 */
export async function cargarAS3(
  filePath: string,
  s3Key: string,
  contentType: string,
  metadata?: Record<string, string>
): Promise<void> {
  try {
    console.log(`Cargando archivo ${filePath} a S3...`);

    // Si no se proporcionan metadatos, crear algunos por defecto
    const metadataToUse = metadata || {
      uploadDate: new Date().toISOString(),
      fileName: filePath.split("/").pop() || "",
      fileType: contentType,
    };

    const resultado = await s3Service.uploadFileFromPath(
      s3Key,
      filePath,
      contentType,
      metadataToUse
    );

    console.log(`Archivo cargado exitosamente a S3:`);
    console.log(`- Bucket: ${process.env.BUCKET_NAME}`);
    console.log(`- Key: ${resultado.key}`);
    console.log(`- Metadatos: ${JSON.stringify(metadataToUse)}`);

    // Generar URL prefirmada para acceder al archivo
    const urlResult = await s3Service.getSignedUrl(s3Key);
    console.log(`- URL de acceso: ${urlResult.signedUrl}`);
  } catch (error) {
    console.error("Error al cargar el archivo CSV a S3:", error);
  }
}

export async function cargarNDJSON(
  s3Key: string,
  metadata?: Record<string, string>
) {
  const user = {
    name: "Esteban",
    age: 30,
    childrens: [
      {
        name: "juli",
        age: 6,
      },
    ],
  };

  const ndjsonContent = [
    { ...user, id: 1 },
    { ...user, id: 2 },
    { ...user, id: 3 },
  ]
    .map((obj) => JSON.stringify(obj))
    .join("\n");

  const resultado = await s3Service.uploadFile(
    s3Key,
    ndjsonContent,
    "application/ndjson",
    metadata
  );

  console.log(`Archivo cargado exitosamente a S3:`);
  console.log(`- Bucket: ${process.env.BUCKET_NAME}`);
  console.log(`- Key: ${resultado.key}`);
  console.log(`- Metadatos: ${JSON.stringify(resultado.metadata)}`);

  // Generar URL prefirmada para acceder al archivo
  const urlResult = await s3Service.getSignedUrl(s3Key);
  console.log(`- URL de acceso: ${urlResult.signedUrl}`);
}

/*interface UserRow {
  id: number;
  name: string;
  email: string;
  age: number;
}*/

export async function leerCSV(s3Key: string) {
  const s3Response = await s3Service.getFile(s3Key);

  if (!s3Response.Body) {
    throw new Error("No se pudo obtener el stream");
  }

  // Mostrar los metadatos del archivo
  console.log("Metadatos del archivo:", s3Response.Metadata);
  /*
  await CsvProcessor.processByChunks(s3Response.Body.transformToWebStream(), {
    batchSize: 10,
    headers: true,
    skipInvalidRows: false,
    onHeaders: (headers) => {
      console.log("onHeaders", headers);
    },
    onValidate: (row) => {
      return row.prueba !== "2323";
    },
    onDataInvalid(row, rowNumber) {
      console.log("onDataInvalid", row, rowNumber);
    },
    onChunk: async (rows) => {
      console.log("onChunk start", rows.length, "=".repeat(6));
      console.log(rows.map((row) => JSON.stringify(row)));
      console.log("onChunk end", "=".repeat(6));
    },
  });

  await CsvProcessor.processByChunks(s3Response.Body.transformToWebStream(), {
    batchSize: 10,
    onChunk: (rows) => {
      console.log("onChunk", rows.length);
    },
  });*/

  await FileProcessor.readInChunks(s3Response.Body, {
    batchSize: 2,
    skipInvalid: true,
    onChunk: async (chunk, index) => {
      console.log("onChunk", {
        index,
        chunk: chunk.length,
      });
    },
  });
}

export async function leerCSVNoOptimizado(s3Key: string) {
  const s3Response = await s3Service.getFile(s3Key);

  if (!s3Response.Body) {
    throw new Error("No se pudo obtener el stream");
  }

  const data = await s3Response.Body.transformToString();

  const rows = data.split("\n");
  const header = data[1].split(",");
  console.log("header", header);
  rows.unshift();
  for (const row of rows) {
    const data = row.split(",");
    console.log(JSON.stringify(data));
  }
  console.log(rows.length);
  console.log("finished");
}
