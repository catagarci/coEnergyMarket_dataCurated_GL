
"""
Configuración de hudi
"""
def get_hudi_options(catalog_database, curated_table_name):
    """
    Devuelve las opciones de configuración para la escritura en Hudi.

    Args:
        process_date (str): Fecha de procesamiento en formato 'YYYYMMDD'.
        catalog_database (str): Nombre de la base de datos en el catálogo de Glue.
        curated_table_name (str): Nombre de la tabla a usar para Hudi.

    Returns:
        dict: Opciones de configuración de Hudi.
    """
    return {
        "hoodie.table.name": f"{curated_table_name}".lower(),
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "bulk_insert",
        "hoodie.datasource.write.recordkey.field": "primary_key",
        "hoodie.datasource.write.precombine.field": "primary_key",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": f"{catalog_database}",
        "hoodie.datasource.hive_sync.table": f"{curated_table_name}".lower(),
        "hoodie.datasource.hive_sync.support_timestamp": "true",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
        "mode": "append",
        "hoodie.datasource.hive_sync.partition_fields": "year,month,day",
        "hoodie.datasource.write.partitionpath.field": "year,month,day",
    }
