import yaml
import sys
import os


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


with open('config/wnvoutbreak.yaml') as f:
    config_dict_temp = yaml.load(f, Loader=yaml.FullLoader)
    config_dict_temp['root'] = pwd()

config_dict = {key: set_path(config_dict_temp['root'], value) if key.endswith('_dir') else value for key, value in
               config_dict_temp.items()}
