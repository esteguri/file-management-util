import { join } from "path";
import { cargarAS3, cargarNDJSON, leerCSV } from ".";

async function probarCsv() {
  // Crear directorio data si no existe
  const dataDir = join(process.cwd(), "data");
  // Ruta al archivo CSV
  //const s3Key = "usuarios-size.csv";
  const s3Key = "usuarios-size.ndjson";
  const csvFilePath = join(dataDir, s3Key);

  try {
    // 1. Cargar el archivo CSV a S3 con metadatos
    const metadata = {
      author: "Usuario",
      description: "Archivo de usuarios para pruebas",
      version: "1.0",
      createdAt: new Date().toISOString(),
    };

    //await cargarAS3(csvFilePath, s3Key, "text/csv", metadata);
    await cargarNDJSON(s3Key, metadata);

    console.log("\n" + "=".repeat(60));
    console.log("🔄 INICIANDO LECTURA OPTIMIZADA POR CHUNKS");
    console.log("=".repeat(60));

    // Time
    const startTime = Date.now();
    await leerCSV(s3Key);
    const endTime = Date.now();
    console.log(`Tiempo de ejecución optimizado: ${endTime - startTime} ms`);
  } catch (error) {
    console.error("❌ Error en el proceso principal:", error);
  }
}

/**
 * Función principal que se ejecuta al iniciar la aplicación
 */
async function main() {
  await probarCsv();
}

// Ejecutar la función principal
main().catch((error) => {
  console.error("Error en la ejecución principal:", error);
});
