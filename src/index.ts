import { s3Service } from "./services/s3-service";
import { existsSync, mkdirSync } from "fs";
import { join } from "path";
import { CsvProcessor } from "./processors/csv.processor";

console.log("¡Hola desde TypeScript!");
console.log("Bucket name:", process.env.BUCKET_NAME);
console.log("Localstack endpoint:", process.env.LOCALSTACK_ENDPOINT);

/**
 * Función para cargar un archivo CSV a S3
 * @param filePath - Ruta local del archivo CSV
 * @param s3Key - Nombre/ruta del archivo en S3
 */
async function cargarCSVaS3(filePath: string, s3Key: string): Promise<void> {
  try {
    console.log(`Cargando archivo ${filePath} a S3...`);

    const resultado = await s3Service.uploadFileFromPath(
      s3Key,
      filePath,
      "text/csv"
    );

    console.log(`Archivo cargado exitosamente a S3:`);
    console.log(`- Bucket: ${process.env.BUCKET_NAME}`);
    console.log(`- Key: ${resultado.key}`);

    // Generar URL prefirmada para acceder al archivo
    const urlResult = await s3Service.getSignedUrl(s3Key);
    console.log(`- URL de acceso: ${urlResult.signedUrl}`);
  } catch (error) {
    console.error("Error al cargar el archivo CSV a S3:", error);
  }
}

interface UserRow {
  id: string;
  name: string;
  email: string;
  age: number;
}

async function leerCSV(s3Key: string) {
  const s3Response = await s3Service.getFile(s3Key);

  if (!s3Response.Body) {
    throw new Error("No se pudo obtener el stream");
  }

  await CsvProcessor.processByChunks<UserRow>(
    s3Response.Body.transformToWebStream(),
    {
      batchSize: 500000,
      headers: true,
      skipInvalidRows: true,
      onHeaders: (headers) => {
        console.log("onHeaders", headers);
      },
      onValidate: (row) => {
        return row.id !== "27";
      },
      onDataInvalid(row, rowNumber) {
        console.log("onDataInvalid", row, rowNumber);
      },
      onChunk: async (rows) => {
        console.log("onChunk start", rows.length, "=".repeat(6));
        console.log(rows.map((row) => JSON.stringify(row)));
        console.log("onChunk end", "=".repeat(6));
      },
    }
  );
}

async function leerCSVNoOptimizado(s3Key: string) {
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

/**
 * Función principal que se ejecuta al iniciar la aplicación
 */
async function main() {
  // Crear directorio data si no existe
  const dataDir = join(process.cwd(), "data");
  if (!existsSync(dataDir)) {
    console.log("Creando directorio data...");
    mkdirSync(dataDir);
  }

  // Ruta al archivo CSV
  const s3Key = "usuarios.csv";
  const csvFilePath = join(dataDir, s3Key);

  try {
    // 1. Cargar el archivo CSV a S3
    await cargarCSVaS3(csvFilePath, s3Key);

    console.log("\n" + "=".repeat(60));
    console.log("🔄 INICIANDO LECTURA OPTIMIZADA POR CHUNKS");
    console.log("=".repeat(60));

    // Time
    const startTime = Date.now();
    await leerCSV(s3Key);
    const endTime = Date.now();
    console.log(`Tiempo de ejecución optimizado: ${endTime - startTime} ms`);

    // no optimizado
    /*const startTimeNoOptimizado = Date.now();
    await leerCSVNoOptimizado(s3Key);
    const endTimeNoOptimizado = Date.now();
    console.log(
      `Tiempo de ejecución no optimizado: ${
        endTimeNoOptimizado - startTimeNoOptimizado
      } ms`
    );*/
  } catch (error) {
    console.error("❌ Error en el proceso principal:", error);
  }
}

// Ejecutar la función principal
main().catch((error) => {
  console.error("Error en la ejecución principal:", error);
});
