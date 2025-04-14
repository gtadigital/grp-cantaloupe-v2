import logging
import os
import sys

def setup_logger(name=__name__, log_file="/logs/log.log", default_level=logging.INFO):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Prevent duplicate handlers

    print(f"log file: {log_file}")

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)
    logger.setLevel(default_level)
    logger.addHandler(handler)

    return logger
