import { PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { s3Client } from './s3-client';
import { Readable } from 'stream';
import { createReadStream } from 'fs';

const bucketName = process.env.BUCKET_NAME || 'file-management-bucket';

/**
 * Servicio para gestionar operaciones con archivos en S3
 */
export class S3Service {
  /**
   * Sube un archivo a S3
   * @param key - Nombre/ruta del archivo en S3
   * @param body - Contenido del archivo (Buffer, string, stream)
   * @param contentType - Tipo de contenido del archivo
   * @param metadata - Metadatos del archivo (pares clave-valor)
   * @returns Promise con el resultado de la operación
   */
  async uploadFile(key: string, body: Buffer | string | Readable, contentType?: string, metadata?: Record<string, string>) {
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: body,
      ContentType: contentType,
      Metadata: metadata
    });

    try {
      const response = await s3Client.send(command);
      return {
        success: true,
        key,
        response,
        metadata
      };
    } catch (error) {
      console.error('Error al subir archivo a S3:', error);
      throw error;
    }
  }

  /**
   * Sube un archivo desde el sistema de archivos local a S3
   * @param key - Nombre/ruta del archivo en S3
   * @param filePath - Ruta local del archivo
   * @param contentType - Tipo de contenido del archivo
   * @param metadata - Metadatos del archivo (pares clave-valor)
   * @returns Promise con el resultado de la operación
   */
  async uploadFileFromPath(key: string, filePath: string, contentType?: string, metadata?: Record<string, string>) {
    const fileStream = createReadStream(filePath);
    return this.uploadFile(key, fileStream, contentType, metadata);
  }

  /**
   * Obtiene un archivo de S3
   * @param key - Nombre/ruta del archivo en S3
   * @returns Promise con el contenido del archivo y sus metadatos
   */
  async getFile(key: string) {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key
    });

    try {
      const response = await s3Client.send(command);
      // Los metadatos están disponibles en response.Metadata
      return response;
    } catch (error) {
      console.error('Error al obtener archivo de S3:', error);
      throw error;
    }
  }

  /**
   * Obtiene los metadatos de un archivo en S3
   * @param key - Nombre/ruta del archivo en S3
   * @returns Promise con los metadatos del archivo
   */
  async getMetadata(key: string) {
    const response = await this.getFile(key);
    return {
      key,
      metadata: response.Metadata || {}
    };
  }

  /**
   * Genera una URL prefirmada para acceder a un archivo
   * @param key - Nombre/ruta del archivo en S3
   * @param expiresIn - Tiempo de expiración en segundos (por defecto 3600 = 1 hora)
   * @returns Promise con la URL prefirmada
   */
  async getSignedUrl(key: string, expiresIn = 3600) {
    const command = new GetObjectCommand({
      Bucket: bucketName,
      Key: key
    });

    try {
      const signedUrl = await getSignedUrl(s3Client, command, { expiresIn });
      return {
        success: true,
        key,
        signedUrl
      };
    } catch (error) {
      console.error('Error al generar URL prefirmada:', error);
      throw error;
    }
  }

  /**
   * Elimina un archivo de S3
   * @param key - Nombre/ruta del archivo en S3
   * @returns Promise con el resultado de la operación
   */
  async deleteFile(key: string) {
    const command = new DeleteObjectCommand({
      Bucket: bucketName,
      Key: key
    });

    try {
      const response = await s3Client.send(command);
      return {
        success: true,
        key,
        response
      };
    } catch (error) {
      console.error('Error al eliminar archivo de S3:', error);
      throw error;
    }
  }
}

// Exportar una instancia del servicio para facilitar su uso
export const s3Service = new S3Service();