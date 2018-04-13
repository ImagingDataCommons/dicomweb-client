'''Utility functions for logging configuration'''
import sys
import logging


def _map_logging_verbosity(verbosity):
    '''Maps logging verbosity to logging level.

    Parameters
    ----------
    verbosity: int
        logging verbosity (e.g. ``2``)

    Returns
    -------
    int
        logging level (e.g. ``logging.INFO``)

    '''
    levels = (logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG)
    try:
        return levels[verbosity]
    except IndexError:
        return levels[-1]


def configure_logging(verbosity):
    '''Configures the root logger with a "stderr" stream handler that directs
    logging messages to standard error (to allow capturing program standard
    output, e.g. in order to redirect it to a file).

    Logging verbosity maps to levels as follows::

            0 -> no messages
            1 -> CRITICAL, ERROR & WARN/WARNING messages
            2 -> CRITICAL, ERROR, WARN/WARNING, & INFO messages
            3 -> CRITICAL, ERROR, WARN/WARNING, INFO & DEBUG messages
            4 -> all messages

    Parameters
    ----------
    verbosity: int
        logging verbosity

    Returns
    -------
    logging.Logger
        package root logger

    '''
    if verbosity > 3:
        fmt = (
            '%(asctime)s | %(levelname)-8s | %(name)-40s | '
            '%(lineno)-4s | %(message)s'
        )
    else:
        fmt = '%(asctime)s | %(levelname)-8s | %(name)-40s | %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.name = 'stderr'
    stderr_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.addHandler(stderr_handler)
    level = _map_logging_verbosity(verbosity)
    root_logger.setLevel(logging.ERROR)

    pkg_name = __name__.split('.')[0]
    pkg_logger = logging.getLogger(pkg_name)
    pkg_logger.setLevel(level)

    return pkg_logger
