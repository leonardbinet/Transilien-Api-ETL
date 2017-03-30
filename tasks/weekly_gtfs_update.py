if __name__ == '__main__':
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from api_etl.utils_misc import set_logging_conf
    set_logging_conf(log_name="download_gtfs_files")

    from api_etl.extract_schedule import ScheduleExtractorRDB

    logger = logging.getLogger(__name__)

    # This operation is done every week
    logger.info("Task: weekly update of gtfs files in two steps:"
                + "download, then save in database.")

    schex = ScheduleExtractorRDB()

    logger.info("Download files.")
    schex.download_gtfs_files()

    logger.info("Save in database.")
    schex.save_in_rdb()
