if __name__ == '__main__':
    import os
    import logging
    from os import sys, path

    BASE_DIR = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_01_w_extract_schedule.log")

    from api_transilien_manager.mod_01_extract_schedule import download_gtfs_files

    logger = logging.getLogger(__name__)

    # This operation is done every week
    logger.info("Task: weekly update of gtfs files")

    logger.info("Download files.")
    download_gtfs_files()
