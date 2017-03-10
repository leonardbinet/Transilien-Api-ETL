if __name__ == '__main__':
    import logging
    from os import sys, path
    import datetime

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.utils_misc import set_logging_conf
    set_logging_conf(log_name="extract_day_schedule")

    from api_etl.settings import dynamo_sched_dep

    from api_etl.utils_misc import get_paris_local_datetime_now
    from api_etl.extract_schedule import dynamo_save_stop_times_of_day_adapt_provision

    logger = logging.getLogger(__name__)

    # Save for next day
    logger.info("Task: daily update of scheduled departures: for tomorrow")

    today_paris = get_paris_local_datetime_now()
    tomorrow_paris = today_paris + datetime.timedelta(days=1)
    tomorrow_paris_str = tomorrow_paris.strftime("%Y%m%d")
    after_tomorrow_paris = today_paris + datetime.timedelta(days=2)
    after_tomorrow_paris_str = after_tomorrow_paris.strftime("%Y%m%d")

    logger.info(
        "Paris tomorrow date is %s, update in schedule table, with day after as well", tomorrow_paris_str)
    days = [tomorrow_paris_str, after_tomorrow_paris_str]
    dynamo_save_stop_times_of_day_adapt_provision(
        days, table_name=dynamo_sched_dep)
