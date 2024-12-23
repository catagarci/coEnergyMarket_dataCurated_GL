import boto3
import pandas as pd
import os
from datetime import datetime
import logging
import sys

class LoadEnergyRawData:
    """
    Clase para cargar archivos CSV desde el sistema local a un bucket S3 de AWS. 
    Los archivos son particionados por fecha (basada en el nombre del archivo) y se 
    almacenan en un prefijo especificado dentro del bucket de S3.

    Atributos:
    -----------
    csv_file_path: str
        Ruta local del archivo CSV que se quiere cargar.
    s3_bucket: str
        Nombre del bucket de S3 donde se almacenará el archivo.
    s3_prefix: str
        Prefijo dentro del bucket de S3 donde se organizarán los archivos 
        (generalmente por carpetas).
    aws_profile: str, opcional
        Perfil de AWS que se usará para autenticar la sesión de boto3 
        (por defecto es 'new_access').

    Métodos:
    --------
    __init__(self, csv_file_path, s3_bucket, s3_prefix, aws_profile='new_access'):
        Constructor de la clase que configura la ruta del archivo, el bucket S3 y el prefijo.
    
    upload_csv_to_s3(self):
        Método que lee un archivo CSV, extrae la fecha de su nombre y lo sube a S3 
        particionado por fecha.
    """

    def __init__(self, csv_file_path, s3_bucket, s3_prefix, aws_profile='new_access'):
        """
        Inicializa la clase con los parámetros necesarios para cargar el archivo a S3.

        Parámetros:
        -----------
        csv_file_path: str
            Ruta local del archivo CSV que se quiere cargar.
        s3_bucket: str
            Nombre del bucket S3 donde se almacenará el archivo.
        s3_prefix: str
            Prefijo dentro del bucket de S3 donde se organizarán los archivos
            (generalmente por carpetas).
        aws_profile: str, opcional
            Perfil de AWS que se usará para autenticar la sesión de boto3. El valor
            por defecto es 'new_access'.
        """
        # Configuración del logger para registrar el progreso y errores
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Verificar si el logger ya tiene manejadores (para evitar repetidos)
        if not self.logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        # Parámetros de entrada
        self.csv_file_path = csv_file_path
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_profile = aws_profile

    def upload_csv_to_s3(self):
        """
        Método que sube el archivo CSV a S3. Primero lee el archivo CSV, luego 
        extrae la fecha del nombre del archivo, y finalmente sube el archivo a S3 
        en la ubicación correspondiente, particionado por fecha (año, mes, día).

        La fecha debe estar en el formato 'yyyyMMdd' en el nombre del archivo.

        Excepciones:
        -------------
        - Si el archivo CSV no puede ser leído, se registra un error.
        - Si la fecha no puede ser extraída del nombre del archivo, se registra un error.
        - Si ocurre un error al subir el archivo a S3, se registra un error.
        """
        # Cargar el archivo CSV
        try:
            df = pd.read_csv(self.csv_file_path)
            self.logger.info("Archivo %s cargado exitosamente en memoria.", 
                             os.path.basename(self.csv_file_path))
        except Exception as e:
            self.logger.error("Error al leer el archivo CSV: %s", e)
            return

        # Obtener la fecha del archivo (se espera que esté en formato yyyyMMdd)
        file_name = os.path.basename(self.csv_file_path)
        try:
            # Extraer la fecha del nombre del archivo
            date_str = file_name.split('_')[1].split('.')[0]  # Obtener la parte yyyyMMdd
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            self.logger.info("Fecha extraída del nombre del archivo: %s", 
                             date_obj.strftime('%Y-%m-%d'))
        except (IndexError, ValueError):
            self.logger.error("El nombre del archivo '%s' no tiene el formato "
                             "esperado 'Providers_yyyyMMdd.csv'.", file_name)
            return

        # Crear el prefijo de S3 para particionar por fecha
        year_folder = date_obj.strftime('%Y')
        month_folder = date_obj.strftime('%m')
        day_folder = date_obj.strftime('%d')

        # Componer la ruta completa en S3 con el separador adecuado
        s3_path = os.path.join(self.s3_prefix, year_folder, month_folder, 
                               day_folder, file_name)

        s3_path = s3_path.replace(os.sep, '/')

        # Inicializar la sesión de boto3 con el perfil especificado
        session = boto3.Session(profile_name=self.aws_profile)
        s3_client = session.client('s3')

        # Subir el archivo a S3
        try:
            with open(self.csv_file_path, 'rb') as data:
                s3_client.upload_fileobj(data, self.s3_bucket, s3_path)
            self.logger.info("Archivo %s subido exitosamente a S3 en: s3://%s/%s", 
                             file_name, self.s3_bucket, s3_path)
        except Exception as e:
            self.logger.error("Error al subir el archivo a S3: %s", e)

# Uso de la clase
if __name__ == "__main__":
    """
    Este bloque permite ejecutar el script desde la línea de comandos, donde
    se debe pasar una fecha en el formato 'yyyyMMdd' como argumento para procesar
    los archivos correspondientes.
    """
    # Verificar que el argumento de fecha esté presente
    if len(sys.argv) != 2:
        print("Error: Se requiere una fecha como argumento (formato yyyyMMdd).")
        exit(1)
    
    process_date = sys.argv[1]

    # Verificar que el formato de la fecha sea válido (yyyyMMdd)
    try:
        # Intentar convertir la fecha ingresada en un objeto datetime para validarla
        datetime.strptime(process_date, '%Y%m%d')
    except ValueError:
        print("Error: El formato de la fecha no es válido. Asegúrese de usar "
              "el formato yyyyMMdd.")
        exit(1)

    # Parámetros de S3
    s3_bucket = 'co-energy-marketer-raw-569587453910-pdn'  # Nombre del bucket S3 sin "s3://"

    # Definir tres diferentes prefijos y nombres de archivo, con la misma fecha
    file_info = [
        ('Providers', f'Providers_{process_date}.csv'),
        ('Customers', f'Customers_{process_date}.csv'),
        ('Transactions', f'Transactions_{process_date}.csv')
    ]
    
    # Subir cada archivo con su respectivo prefijo
    for prefix, file_name in file_info:
        # Construir el path del archivo CSV
        csv_file_path = f'data/{file_name}'
        
        # Crear una instancia de la clase y subir el archivo
        loader = LoadEnergyRawData(csv_file_path, s3_bucket, prefix)
        loader.upload_csv_to_s3()
