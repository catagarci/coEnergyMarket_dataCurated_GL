"""
Configuración de logger
"""
import logging

def create_customer_logger(logger_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Crea un logger personalizado para la aplicación.

    Args:
        logger_name (str): Nombre del logger, normalmente el nombre del módulo o de la clase.
        log_level (str): Nivel de log. Puede ser 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'. Por defecto es 'INFO'.

    Returns:
        logging.Logger: Un objeto logger configurado.
    """

    # Configurar el nivel de log por defecto
    log_level = log_level.upper()

    # Crear un logger con el nombre proporcionado
    logger = logging.getLogger(logger_name)

    # Establecer el nivel de log
    logger.setLevel(log_level)

    # Crear un formateador para definir cómo se verán los logs
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Crear un manejador de logs para la consola
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Agregar el manejador al logger
    logger.addHandler(console_handler)

    return logger
