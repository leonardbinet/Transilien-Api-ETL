if __name__ == '__main__':

    import os
    import sys
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    # Logging configuration
    from api_transilien_manager.utils_misc import set_logging_conf
    set_logging_conf(log_name="task_01_single_cycle.log")

    from api_transilien_manager.mod_01_extract_api import operate_one_cycle

    logger = logging.getLogger(__name__)

    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning single cycle extraction")
    operate_one_cycle(station_filter=False, max_per_minute=300)
