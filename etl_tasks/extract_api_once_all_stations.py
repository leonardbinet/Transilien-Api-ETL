if __name__ == '__main__':
    import sys
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.extract_api import operate_one_cycle
    from api_etl.utils_misc import set_logging_conf

    set_logging_conf(log_name="extract_api_once_all_stations")
    logger = logging.getLogger(__name__)

    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning single cycle extraction")
    operate_one_cycle(station_filter=False)
