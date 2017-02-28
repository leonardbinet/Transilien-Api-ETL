if __name__ == '__main__':
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from api_etl.utils_misc import set_logging_conf
    module_name = sys.modules[__name__]
    set_logging_conf(log_name=module_name)

    from api_etl.extract_schedule import download_gtfs_files

    logger = logging.getLogger(__name__)

    # This operation is done every week
    logger.info("Task: weekly update of gtfs files")

    logger.info("Download files.")
    download_gtfs_files()
