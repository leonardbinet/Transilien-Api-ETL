if __name__ == '__main__':
    import sys
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.extract_api import operate_one_cycle
    from api_etl.utils_misc import set_logging_conf

    module_name = sys.modules[__name__]
    set_logging_conf(log_name=module_name)
    logger = logging.getLogger(__name__)

    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning single cycle extraction")
    operate_one_cycle(station_filter=False, max_per_minute=300)
