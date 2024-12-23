"""Transform"""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql import DataFrame
from extra.utils.utils import create_customer_logger

# Configuración del logger
logger = create_customer_logger("tables_energy", "DEBUG")


class EnergyMarketTransform:
    """
    Clase que contiene métodos para transformar datos del mercado de energía.
    """

    @staticmethod
    def set_cast_columns(df: DataFrame) -> DataFrame:
        """
        Función que castea las columnas 'nombre_proveedor' y 'tipo_energia' de
        un DataFrame a StringType.

        Parámetros:
        df -- DataFrame de entrada

        Retorna:
        DataFrame con las columnas casteadas
        """
        try:
            # Casteo de las columnas
            df_casteado = df.withColumn('nombre_proveedor', df['nombre_proveedor']
                                       .cast(StringType())) \
                            .withColumn('tipo_energia', df['tipo_energia']
                                       .cast(StringType()))

            return df_casteado
        except Exception as e:
            logger.error("Error en set_cast_columns: %s", e)
            raise

    @staticmethod
    def set_cast_columns_customers(df: DataFrame) -> DataFrame:
        """
        Función que castea las columnas 'nombre_proveedor' y 'tipo_energia' de
        un DataFrame a StringType.

        Parámetros:
        df -- DataFrame de entrada

        Retorna:
        DataFrame con las columnas casteadas
        """
        try:
            # Casteo de las columnas
            df_casteado = df.withColumn('tipo_identificacion', df['tipo_identificacion']
                                       .cast(StringType())) \
                            .withColumn('identificacion', df['identificacion']
                                       .cast(StringType())) \
                            .withColumn('nombre', df['nombre']
                                       .cast(StringType())) \
                            .withColumn('ciudad', df['ciudad']
                                       .cast(StringType()))

            return df_casteado
        except Exception as e:
            logger.error("Error en set_cast_columns_customers: %s", e)
            raise

    @staticmethod
    def set_cast_columns_transactions(df: DataFrame) -> DataFrame:
        """
        Función que castea las columnas 'nombre_proveedor' y 'tipo_energia' de
        un DataFrame a StringType.

        Parámetros:
        df -- DataFrame de entrada

        Retorna:
        DataFrame con las columnas casteadas
        """
        try:
            # Casteo de las columnas
            df_casteado = df.withColumn('tipo_transaccion', df['tipo_transaccion']
                                       .cast(StringType())) \
                            .withColumn('nombre', df['nombre']
                                       .cast(StringType())) \
                            .withColumn('cantidad_comprada', df['cantidad_comprada']
                                       .cast(IntegerType())) \
                            .withColumn('precio', df['precio']
                                       .cast(IntegerType())) \
                            .withColumn('tipo_energia', df['tipo_energia']
                                       .cast(StringType())) \

            return df_casteado

        except Exception as e:
            logger.error("Error en set_cast_columns_transactions: %s", e)
            raise

    @staticmethod
    def set_columns_transformation_upper(df: DataFrame, column_name: str) -> DataFrame:
        """
        Función que convierte a mayúsculas las columnas especificadas.

        Parámetros:
        df -- DataFrame de entrada
        column_name -- nombre de la columna a modificar

        Retorna:
        DataFrame con las columnas modificadas
        """
        try:
            df = df.withColumn(column_name, F.upper(F.col(column_name)))
            return df

        except Exception as e:
            logger.error("Error en set_columns_transformation_upper: %s", e)
            raise

    @staticmethod
    def replace_nulls_with_zero(df, column_name):
        """
        Reemplaza los valores nulos (vacíos) en una columna específica de un
        DataFrame de PySpark por cero.

        Parámetros:
        df (DataFrame): El DataFrame de PySpark en el que se realizarán las
        modificaciones.
        column_name (str): El nombre de la columna en la que se deben reemplazar
        los valores nulos por cero.

        Retorna:
        DataFrame: Un nuevo DataFrame con los valores nulos de la columna
        especificada reemplazados por cero. Si la columna no existe o hay un error,
        devuelve None.

        Excepciones:
        ValueError: Si la columna especificada no existe en el DataFrame.
        Exception: Captura cualquier otro error que ocurra durante el proceso.
        """
        try:
            # Verificar si la columna existe en el DataFrame
            if column_name not in df.columns:
                raise ValueError(f"La columna '{column_name}' no existe en el DataFrame.")

            # Reemplazar los valores nulos en la columna especificada con 0
            df_updated = df.fillna({column_name: 0})

            return df_updated

        except Exception as e:
            logger.error("Error en replace_nulls_with_zero: %s", e)
            return None
