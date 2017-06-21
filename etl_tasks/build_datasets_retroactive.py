if __name__ == '__main__':
    import sys
    import logging
    from os import sys, path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

    from api_etl.matrix_builder import TrainingSetBuilder

    logger = logging.getLogger(__name__)

    # Default: all stations, and max 300 queries per sec
    logger.info("Beginning building of training sets")

    tsb = TrainingSetBuilder(start="20170306", end="20170615", tempo=30)
    tsb.create_training_sets()
