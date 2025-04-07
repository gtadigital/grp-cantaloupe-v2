"""
Download and format XML data from an EasyDB GTA instance.

This script authenticates with the EasyDB system, downloads item data from a specific module 
(e.g., 'person', 'group'), saves the data as XML files, and formats the files by cleaning 
unwanted attributes. It also updates metadata for future sync operations.

Usage:
    python download_data_from_easydb.py --login <login> --password <password> \
        --module <module_name> --base_folder <folder> [--filenamePrefix <prefix>] [--downloadWhat <type>]

Arguments:
    --login           (str)  : EasyDB login email (required)
    --password        (str)  : EasyDB account password (required)
    --module          (str)  : Name of the module to download (e.g., "person", "group") (required)
    --base_folder     (str)  : Root folder where XML files will be saved (required)
    --filenamePrefix  (str)  : Prefix for generated XML files (optional, default: "item-")
    --downloadWhat    (str)  : Download mode ("all", "update", or "sample"; optional, default: "update")

Download Modes:
    - "all"     : Downloads all items, ignoring last sync time.
    - "update"  : Downloads only updated items since the last recorded sync (default).
    - "sample"  : Downloads a small subset of sample data for testing purposes.

Example:
    python download_data_from_easydb.py \
        --login user@example.com \
        --password secret123 \
        --module person \
        --base_folder ./data/ \
        --filenamePrefix person- \
        --downloadWhat all
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
    """
    if path[-1] != '/':
        return path + '/'
    return path

def format_file(xml):
    """
    Formats an XML file by removing attributes from the <objects> tag.
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
    Output: Formats `file1.xml`, `file2.xml`, and `file3.xml`.
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
    Main function to handle authentication, downloading, formatting, and metadata updates.
    """
    download_path = sanitize_path(base_folder + objecttype)
    
    ezdb = Session(EASYDB_URL)
    start_session(ezdb)
    ezdb._setLogin(login)
    ezdb._setPassword(password)
    authenticate_session(ezdb)

    print("base_folder:", base_folder)
    print("path:", download_path)

    metadata = ItemMetadata(base_folder)

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