"""
Script to download data from easydb GTA instance.

Usage:
    python download_data_from_easydb.py --login <login> --password <password> --module <module> --path <path> --base_folder <base_folder> --filenamePrefix <filenamePrefix> --downloadWhat <downloadWhat>

Arguments:
    --login: Login email for GTA instance account
    --password: Login password for GTA instance account
    --module: Name of the module to download items from
    --path: Folder where all downloaded files will be stored
    --base_folder: Base folder where the object files and metadata file are saved
    --filenamePrefix: Prefix to use for the filenames of the XML files. Defaults to "item-"
    --downloadWhat: What data to download ("all", "update", or "sample"). Defaults to "update"
"""

import argparse
import sys
import pathlib
import pytz
from datetime import datetime, timezone
# from tqdm import tqdm
import re

sys.path.append( 'utils' )
from easydb import Session, start_session, authenticate_session, deauthenticate_session, run_export_pipeline
from lib.Metadata import ItemMetadata

EASYDB_URL = 'https://collections.gta.arch.ethz.ch'

def sanitize_path(path):
    """
    Ensures that a given file path ends with a forward slash ('/').

    This function checks whether the input path ends with a forward slash. 
    If it does not, the function appends a forward slash to the path.

    Args:
        path (str): The file or directory path to sanitize.

    Returns:
        str: The sanitized path, guaranteed to end with a forward slash.

    Example:
        Input: "folder/subfolder"
        Output: "folder/subfolder/"
    """
    if path[-1] != '/':
        return path + '/'
    return path

def format_file(xml):
    """
    Formats an XML file by removing attributes from the <objects> tag.

    This function reads the content of an XML file, modifies it to remove any
    attributes from the <objects> tag (leaving only the tag itself), and then
    writes the updated content back to the file.

    Args:
        xml (str): The file path of the XML file to format.

    Example:
        Input:  <objects id="123" type="example">...</objects>
        Output: <objects>...</objects>
    """
    with open(xml, "r", encoding="utf-8") as file:
        xml_content = file.read()

    # Remove attributes from the <objects> tag, leaving just <objects>
    xml_content = re.sub(r'<objects\b[^>]*>', '<objects>', xml_content)

    with open(xml, "w", encoding="utf-8") as file:
        file.write(xml_content)

def format_all_files(path):
    """
    Formats all XML files in a given directory and its subdirectories.

    This function recursively scans the specified directory for XML files and applies the `format_file`
    function to each one. It also keeps track of directories encountered during the traversal, ensuring 
    that only unique and deepest-level directories are stored.

    Args:
        path (str): The root directory path to scan for XML files and subdirectories.

    Behavior:
        - Applies `format_file` to all files ending with `.xml`.
        - Tracks and maintains a list of unique directories.

    Example:
        Input:
            A directory containing:
                folder/
                ├── subfolder/
                │   ├── file1.xml
                │   └── file2.xml
                └── file3.xml

        Output:
            Formats `file1.xml`, `file2.xml`, and `file3.xml`.
    """
    paths = []
    for subpath in pathlib.Path(path).rglob("*"):
        if str(subpath)[-3:] == 'xml':
            format_file(str(subpath))
        if subpath.is_dir():
            if subpath.parent in paths:
                paths.remove(subpath.parent)
            paths.append(subpath)

def main(*, login, password, objecttype, base_folder, limit=None, filenamePrefix='item-', downloadWhat='update'):
    """
    Main function to handle downloading, processing, and formatting data.

    This function orchestrates the download and formatting pipeline for a specific object type 
    from EasyDB. It authenticates the session, manages metadata, and processes the files 
    based on the specified parameters.

    Args:
        login (str): The EasyDB login username.
        password (str): The EasyDB login password.
        objecttype (str): The type of objects to download (e.g., "person", "group").
        base_folder (str): The base folder path for storing downloaded data.
        filenamePrefix (str, optional): The prefix for generated filenames. Defaults to 'item-'.
        downloadWhat (str, optional): Specifies what to download ('update', 'all', or 'sample').
            - "all": Download all available data.
            - "update": Download only updated data since the last sync.
            - "sample": Download a limited sample of data.

    Behavior:
        - Sets up the EasyDB session with authentication.
        - Tracks and logs metadata (e.g., `lastUpdated` timestamp).
        - Determines the appropriate pipeline to run (`run_export_pipeline` or `sample_run_export_pipeline`).
        - Updates metadata for the current session.
        - Formats all XML files in the download directory.

    Example:
        main(
            login="user",
            password="password123",
            objecttype="person",
            base_folder="/data/source/",
            filenamePrefix="person-item-",
            downloadWhat="all"
        )
    """
    download_path = sanitize_path(base_folder + objecttype)
    
    ezdb = Session(EASYDB_URL)
    start_session(ezdb)
    ezdb._setLogin(login)
    ezdb._setPassword(password)
    authenticate_session(ezdb)

    print("base_folder:", base_folder)
    print("path:", download_path)

    metadata = ItemMetadata(download_path)

    # READ DATE FROM THE METADATA FILE
    try:
        lastUpdated = metadata.getLastUpdatedDate()
    except:
        lastUpdated = None
    
    # Set `lastUpdated` to '1970-01-01 00:00:00.000' if downloadWhat is "all" or if lastUpdated does not exist yet
    if downloadWhat == "all" or lastUpdated is None:
        lastUpdated = '1970-01-01 00:00:00.000'

    print(f'lastUpdated: {lastUpdated}')
    
    # Store the start time of the download
    downloadStarted_utc = datetime.now(pytz.utc)
    downloadStarted = downloadStarted_utc.strftime('%Y-%m-%d %H:%M:%S.000')
    print(f'downloadStarted: {downloadStarted}')
    
    run_export_pipeline(ezdb, objecttype, lastUpdated, download_path, limit, metadata, filenamePrefix)   
    
    # Update metadata with the current download start time
    metadata.setLastUpdated(downloadStarted)
    
    # Deauthenticate the session
    deauthenticate_session(ezdb)

    # Format all XML files in the download directory
    format_all_files(download_path)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser = argparse.ArgumentParser(description = 'Download XML data from EasyDB')
    parser.add_argument('--login', required=True,help='login email for the EasyDB portal')
    parser.add_argument('--password', required=True,help='password for login on EasyDB portal')
    parser.add_argument('--module',required=True, help='name of the module to download items from')
    parser.add_argument('--base_folder',required=True, help='base folder')
    parser.add_argument('--filenamePrefix', required= False, help='Prefix to use for the filenames of the XML files. Defaults to "item-"')
    parser.add_argument('--downloadWhat', required=False, default=None, help='Which data to download ("all", "update", or "sample")')
    
    args = parser.parse_args()
    login = args.login
    password = args.password
    module = args.module
    base_folder = args.base_folder

    main(login=login, password=password, objecttype=module, base_folder=base_folder, filenamePrefix=args.filenamePrefix or 'item-', downloadWhat=args.downloadWhat or "update")