if __name__ == '__main__':
    import sys
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.matrix_builder import TrainingSetBuilder
    from api_etl.utils_misc import set_logging_conf

    set_logging_conf(log_name="build-training-sets")
    logger = logging.getLogger(__name__)

    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning building of training sets")

    tsb = TrainingSetBuilder(start="20170225", end="20170501", tempo=30)
    tsb.create_training_sets()
