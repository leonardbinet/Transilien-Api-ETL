if __name__ == '__main__':
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.utils_misc import set_logging_conf
    set_logging_conf(log_name="extract_day_schedule")

    from api_etl.settings import dynamo_sched_dep_all

    from api_etl.extract_schedule import dynamo_save_stop_times_of_day_adapt_provision

    logger = logging.getLogger(__name__)

    logger.info("Saving all stop times in dynamo")

    dynamo_save_stop_times_of_day_adapt_provision(
        "all", table_name=dynamo_sched_dep_all)
