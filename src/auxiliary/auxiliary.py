import dotenv
import logging
import coloredlogs
import os


def load_configs():
    """
    load configs of environment
    :return: log for logging, config is list of all env vars
    """
    config = dotenv.dotenv_values(".env")
    log = logging.getLogger(__name__)
    coloredlogs.install(level=config["LOGLEVEL"])
    return log, config


def build_non_existing_dirs(file_path: str):
    """
    build non existing directories
    :param file_path:
    :return: True
    """
    file_path = os.path.normpath(file_path)
    # Split the path into individual directories
    dirs = file_path.split(os.sep)
    # Check if each directory exists and create it if it doesn't
    current_path = ""
    for directory in dirs:
        current_path = os.path.join(current_path, directory)
        if not os.path.exists(current_path):
            os.mkdir(current_path)
    return True
