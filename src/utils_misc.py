import os
from os import sys
import logging
from logging.handlers import RotatingFileHandler

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))


def set_logging_conf(log_name, level="INFO"):
    """
    This must be imported by all scripts running as "main"
    """
    if level == "INFO":
        level = logging.INFO
    elif level == "DEBUG":
        level = logging.DEBUG
    else:
        level = logging.INFO
    # Delete all previous potential handlers
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Set config
    logging_file_path = os.path.join(
        BASE_DIR, "..", "logs", log_name)

    # création d'un handler qui va rediriger une écriture du log vers
    # un fichier en mode 'append', avec 1 backup et une taille max de 1Mo
    file_handler = RotatingFileHandler(logging_file_path, 'a', 1000000, 1)

    # création d'un second handler qui va rediriger chaque écriture de log
    # sur la console
    stream_handler = logging.StreamHandler()

    handlers = [file_handler, stream_handler]

    logging.basicConfig(
        format='%(asctime)s-- %(name)s -- %(levelname)s -- %(message)s', level=level, handlers=handlers)

    # Python crashes or captured as well (beware of ipdb imports)
    def handle_exception(exc_type, exc_value, exc_traceback):
        # if issubclass(exc_type, KeyboardInterrupt):
        #    sys.__excepthook__(exc_type, exc_value, exc_traceback)
        logging.error("Uncaught exception : ", exc_info=(
            exc_type, exc_value, exc_traceback))
        sys.__excepthook__(exc_type, exc_value, exc_traceback)

    sys.excepthook = handle_exception
