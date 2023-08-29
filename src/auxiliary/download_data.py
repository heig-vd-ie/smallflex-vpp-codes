import os
import owncloud
import dotenv
import tqdm
from auxiliary.auxiliary import build_non_existing_dirs
from zipfile import ZipFile


def download_from_switch(switch_path: str, local_file_path: str, download_anyway: bool = False):
    """
    :param switch_path: switch path that needed to be downloaded
    :param local_file_path: local path of the needed file
    :param download_anyway: a boolean to say download it anyway
    :return True
    """
    local_file_path = os.path.normpath(local_file_path)
    build_non_existing_dirs(local_file_path)
    # Read password and username
    config = dotenv.dotenv_values("../.env")
    public_link = str(config["SWITCH_LINK"])
    pwd = str(config["SWITCH_PASS"])
    # Read cloud
    oc = owncloud.Client.from_public_link(public_link, folder_password=pwd)
    # Go over all files in the cloud and download them one by one
    files = oc.list(switch_path)
    for file in tqdm.tqdm(files, desc="Download files from remote directory " + switch_path, ncols=150):
        file_dir = file.path
        file_name: str = os.path.basename(file_dir)
        if (not os.path.exists(os.path.join(local_file_path, file_name))) | download_anyway:
            oc.get_file(file_dir, os.path.join(local_file_path, file_name))
    return True
