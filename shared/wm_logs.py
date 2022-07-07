import logging

from . import config


def _new_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    fh = logging.FileHandler(config.config_log['log_path'])
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    fh.setFormatter(formatter)
    
    logger.addHandler(fh)
    return logger


logger = _new_logger()
