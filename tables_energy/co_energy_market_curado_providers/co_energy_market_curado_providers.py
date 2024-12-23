"""
Este script es un job de AWS Glue que se encarga de leer datos desde el catálogo
Glue en la zona 'Raw', procesarlos y escribir los datos curados en un almacenamiento
Hudi particionado por año, mes y día.

Parámetros de entrada:
- JOB_NAME: co_energy_market_curado_providers
- PROCESS_DATE: Fecha de procesamiento en formato 'YYYYMMDD'.
- TYPE_PROCESS: Tipo de proceso (por defecto "INC", para proceso incremental).

Este script realiza las siguientes tareas:
1. Lee los datos desde el catálogo Glue.
2. Agrega columnas de partición.
3. Escribe los datos en formato parquet en un entorno particionado.
"""
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import SparkSession, functions as F
from py4j.protocol import Py4JJavaError
from extra.utils.utils import create_customer_logger
from extra.config.transform import EnergyMarketTransform
from extra.config.constants import (
    DOMAIN,
    CAPACITY,
    PAIS,
    CATALOG_TABLE_NAME_PROVIDERS,
    CURATED_TABLE_NAME_PROVIDERS,
    DATA_PRODUCT_PROVIDERS,
    NAME_PROVIDERS,
)
from extra.config.sparkconfig import (
    spark_config,
)
from extra.config.hudiconfig import (
    get_hudi_options,
)

logger = create_customer_logger("tables_energy", "DEBUG")

class EnergyProviders:
    """
    Clase para procesar los datos de los proveedores.
    """
    def __init__(self, args: dict):
        # Inicializa la clase EnergyProviders con el contexto de Glue y los parámetros del job.
        self.args = args
        self.account = args.get("ACCOUNT", "")
        self.env = "qc" if args["ENV"] == "qa" else args["ENV"]
        self.process_date = args.get("PROCESS_DATE", "")
        self.job_name = args.get("JOB_NAME", "")
        self.type_process = args.get("TYPE_PROCESS", "INC")

        # Extraer year, month y day desde PROCESS_DATE
        self.year = self.process_date[:4]
        self.month = self.process_date[4:6]
        self.day = self.process_date[6:8]

        # Crear la sesión de Spark dentro del constructor
        self._spark: SparkSession = (
            SparkSession.builder
            .appName(DATA_PRODUCT_PROVIDERS)
            .enableHiveSupport()
            .config(conf=spark_config)
            .getOrCreate()
        )

        if "pytest" not in sys.modules:
            self.glue_context = GlueContext(self._spark.sparkContext)
        else:
            self.glue_context = None

        # Definir rutas
        self.path_raw_data = (
            f"s3://{PAIS}-{CAPACITY}-{DOMAIN}-raw-{self.account}-{self.env}/"
            f"Providers/{self.year}/{self.month}/{self.day}/{CATALOG_TABLE_NAME_PROVIDERS}_{self.process_date}.csv"
        )

        self.path_curated_data = (
            f"s3://{PAIS}-{CAPACITY}-{DOMAIN}-curated-{self.account}-{self.env}/"
            f"table-catalog/{CURATED_TABLE_NAME_PROVIDERS}/"
        )
        self.catalog_database = f"{PAIS}_{CAPACITY}_{DOMAIN}_curated_{self.env}_rl"

        #Job de Glue
        self.job = Job(self.glue_context)
        self.job.init(self.job_name, self.args)

    def read_raw_data(self):
        """Lee los datos desde la ruta Raw."""
        logger.info("Ruta Raw: %s", self.path_raw_data)
        return self._spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("delimiter", ";") \
            .load(self.path_raw_data)

    def process_data(self, raw_df):
        """Agrega columnas necesarias."""
        # Verificar si hay registros y agregar columnas de particionamiento
        if not raw_df.isEmpty():
            raw_df = raw_df.withColumn("primary_key", F.monotonically_increasing_id() + 1)
            raw_df = raw_df.withColumn("year", F.lit(self.process_date[:4]))
            raw_df = raw_df.withColumn("month", F.lpad(F.lit(self.process_date[4:6]), 2, "0"))
            raw_df = raw_df.withColumn("day", F.lpad(F.lit(self.process_date[6:8]), 2, "0"))
        else:
            logger.info("DataFrame vacío para la fecha de proceso: %s", self.process_date)
        return raw_df

    def write_to_hudi(self, raw_df):
        """Escribe los datos procesados a Hudi."""
        if not raw_df.isEmpty():
            #Aplicar las transformaciones
            raw_df = \
                    EnergyMarketTransform.set_cast_columns(
                    raw_df
            )
            raw_df = \
                    EnergyMarketTransform.set_columns_transformation_upper(
                    raw_df,
                    column_name=NAME_PROVIDERS
            )
            # Llamar a la función para obtener las opciones de Hudi
            hudi_options = get_hudi_options(self.catalog_database, CURATED_TABLE_NAME_PROVIDERS)

            try:
                (
                raw_df.write
                    .format("hudi")
                    .mode('append')
                    .options(**hudi_options)
                    .save(self.path_curated_data)
                )

                logger.info(
                    "Guardado exitoso en Hudi en la base de datos: %s, tabla: %s",
                    self.catalog_database,
                    CURATED_TABLE_NAME_PROVIDERS
                )

            except Py4JJavaError as write_error:
                logger.error("Error al escribir los datos: %s", write_error)
                raise RuntimeError from write_error

    def run(self):
        """Método principal para ejecutar el proceso."""
        raw_df = self.read_raw_data()
        new_df = self.process_data(raw_df)
        self.write_to_hudi(new_df)
        self.job.commit()

# Ejecución del script
if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "ACCOUNT",
        "ENV",
        "PROCESS_DATE",
        "TYPE_PROCESS"])

    # Crear e iniciar el proceso
    energy_providers = EnergyProviders(args)
    energy_providers.run()
