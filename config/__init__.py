import yaml
import sys
import os
import logging


def pwd():
    r"""Prints the working directory.
    Used to determine the directory this module is in.

    Returns:
        The path of the directory this module is in.
    """
    wd = sys.path[0]
    return wd


def set_path(wd, data_path):
    r"""Joins a path to the working directory.

    Arguments:
        wd: The path of the directory this module is in.
        data_path: The suffix path to join to wd.
    Returns:
        The joined path.
    """
    path_name = os.path.join(wd, data_path)
    return path_name


def setup_logging(level='INFO', fn='app.log'):
    r"""Configures the logger Level.
    Arguments:
        level: CRITICAL -> ERROR -> WARNING -> INFO -> DEBUG.
    Side effect:
        The minimum logging level is set.
    """
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    ll = logging.getLevelName(level)
    logger = logging.getLogger()
    handler = logging.FileHandler(fn, mode='a')
    formatter = logging.Formatter(
        "%(asctime)s %(name)-12s %(levelname)-8s"
        "{'file': %(filename)s 'function': %(funcName)s 'line': %(lineno)s}\n"
        "message: %(message)s\n")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(ll)


with open('config/wnvoutbreak.yaml') as f:
    config_dict_temp = yaml.load(f, Loader=yaml.FullLoader)
    config_dict_temp['root'] = pwd()

config_dict = {key: set_path(config_dict_temp['root'], value) if key.endswith('_dir') else value for key, value in
               config_dict_temp.items()}
