import logging
import os
import sys

def setup_logger(name=__name__, log_file="/logs/log.log", default_level=logging.DEBUG):
    """
    Create a logger that writes INFO+ to console and all levels to the log file.
    Prevents duplicate handlers and creates the log directory if needed.
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # Prevent duplicate handlers

    print(f"log file: {log_file}")

    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)   # Only INFO and above printed
    logger.addHandler(console_handler)

    logger.setLevel(default_level)

    return logger
