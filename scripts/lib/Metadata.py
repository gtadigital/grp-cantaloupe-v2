import json
from datetime import datetime
from os import listdir
from os.path import join, exists, isfile
from lxml import etree

class ItemMetadata:
    """
    Class for storing and retrieving metadata about the downloaded items

    Usage:
    >>> # Create a new instance of the class based on the folder where the items are stored
    >>> metadata = ItemMetadata(folder)
    >>> # Get the last updated date
    >>> lastUpdated = metadata.getLastUpdatedDate()
    >>> # Set the last updated date
    >>> metadata.setLastUpdated(datetime.now())
    """
    METADATA_FILENAME = 'metadata.json'

    directory = None
    metadataFile = None

    def __init__(self, directory):
        """
        Initialize the class
        
        args:
            directory (str): The directory where the items are stored
        """
        self.directory = directory
        self.metadataFile = join(directory, self.METADATA_FILENAME)
        self.metadata = self.loadMetadata()

    def getLastIngestedDateForFile(self, filename):
        """
        Get the last ingested date from the metadata file for a specific filename.
        The last ingested date is stored in the key 'lastIngested' in the for the given filename in the 'files' key.
        """
        if 'files' in self.metadata and filename in self.metadata['files'] and 'lastIngested' in self.metadata['files'][filename]:
            return self.metadata['files'][filename]['lastIngested']
        else:
            return None

    def getLastMappedDateForFile(self, filename):
        """
        Get the last mapped date from the metadata file for a specific filename.
        The last mapped date is stored in the key 'lastMapped' in the for the given filename in the 'files' key.
        """
        if 'files' in self.metadata and filename in self.metadata['files'] and 'lastMapped' in self.metadata['files'][filename]:
            return self.metadata['files'][filename]['lastMapped']
        else:
            return None
        
    def getLastUpdatedDateForFile(self, filename):
        """
        Get the last updated date from the metadata file for a specific filename.
        The last updated date is stored in the key 'lastUpdated' in the for the given filename in the 'files' key.
        """
        if 'files' in self.metadata and filename in self.metadata['files'] and 'lastUpdated' in self.metadata['files'][filename]:
            return self.metadata['files'][filename]['lastUpdated']
        else:
            return None
    
    def getLatestImageDownloadUrlForFile(self, filename):
        """
        Get the latest image download url from the metadata file for a specific filename.
        The latest image download url is stored in the key 'latestImageDownloadUrl' in the for the given filename in the 'files' key.
        """
        if 'files' in self.metadata and filename in self.metadata['files'] and 'latestImageDownloadUrl' in self.metadata['files'][filename]:
            return self.metadata['files'][filename]['latestImageDownloadUrl']
        else:
            return None
        
    def getLatestPdfDownloadUrlForFile(self, filename):
        """
        Get the latest pdf download url from the metadata file for a specific filename.
        The latest pdf download url is stored in the key 'latestPdfDownloadUrl' in the for the given filename in the 'files' key.
        """
        if 'files' in self.metadata and filename in self.metadata['files'] and 'latestPdfDownloadUrl' in self.metadata['files'][filename]:
            return self.metadata['files'][filename]['latestPdfDownloadUrl']
        else:
            return None

    def getLastUpdatedDate(self):
        """
        Get the last updated date from the metadata file.
        The last updated date is stored in the key 'lastUpdated' in the metadata file.
        """
        if 'lastUpdated' in self.metadata and self.metadata['lastUpdated'] is not None:
            return self.metadata['lastUpdated']
        else:
            # Get the last updated date from the existing files
            lastUpdated  = self.getLastUpdatedFromItemFiles()
            if lastUpdated:
                self.setLastUpdated(lastUpdated)
                return lastUpdated
    
    def getLastUpdatedFromItemFiles(self):
        """
        Determines the last updated date by reading the __lastModified field from
        all XML files in the input folder and returning the highest value.
        """
        # Read all XML files in the input folder
        files = [f for f in listdir(self.directory) if isfile(join(self.directory, f)) and f.endswith('.xml')]
        
        # If no files exist yet, return None
        if len(files) == 0:
            return None

        # Set lastUpdated to a Date object with the lowest possible value
        lastUpdated = datetime.min
        
        namespace = {'ns': 'https://schema.easydb.de/EASYDB/1.0/objects/'}

        for file in files:
            tree = etree.parse(join(self.directory, file))
            # lastUpdatedString= tree.find('.//{http://www.zetcom.com/ria/ws/module}systemField[@name="__lastModified"]/{http://www.zetcom.com/ria/ws/module}value').text
            lastModified = tree.find('.//ns:_last_modified', namespaces=namespace)
            
            if lastModified is not None:
                lastUpdatedString = lastModified.text
            else:
                lastUpdatedString = None
            
            # lastUpdatedString= tree.find('.//{http://www.zetcom.com/ria/ws/module}systemField[@name="__lastModified"]/{http://www.zetcom.com/ria/ws/module}value').text
            # lastUpdatedItem = datetime.strptime(lastUpdatedString, '%Y-%m-%d %H:%M:%S.%f')
            lastUpdatedItem = datetime.strptime(lastUpdatedString, '%Y-%m-%d %H:%M:%S.000')
            if lastUpdatedItem > lastUpdated:
                lastUpdated = lastUpdatedItem
        # return lastUpdated.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        return lastUpdated.strftime('%Y-%m-%d %H:%M:%S.000')

    def loadMetadata(self):
        """
        Reads the metadata file and returns the contents as a dictionary.
        """
        if exists(self.metadataFile):
            with open(self.metadataFile, 'r') as f:
                try:
                    metadata = json.load(f)
                except:
                    raise Exception(f"Could not read metadata file {self.metadataFile}")
                return metadata
        else:
            # Create an empty metadata file
            with open(self.metadataFile, 'w') as f:
                json.dump({}, f)
            return {}
        
    def listFiles(self):
        files = [d for d in self.metadata['files']]
        return files
    
    def removeFile(self, filename, *, write=True):
        """
        Removes the given file from the metadata.

        args:
            filename (str): The filename to remove
            write (bool, optional): Whether to write the metadata to the metadata file. Defaults to True.
        """
        if 'files' in self.metadata and filename in self.metadata['files']:
            del self.metadata['files'][filename]
        if write:
            self.writeMetadata()

    def setLastUpdated(self, lastUpdated, *, write=True):
        """
        Set the last updated date for the module.
        Adds the key 'lastUpdated' to the metadata if it does not exist yet and
        sets the value to the given lastUpdated date.

        args:
            lastUpdated (str or datetime): The last updated date to set
            write (bool, optional): Whether to write the metadata to the metadata file. Defaults to True.
        """
        if isinstance(lastUpdated, str):
            self.metadata['lastUpdated'] = lastUpdated
        elif isinstance(lastUpdated, datetime):
            # self.metadata['lastUpdated'] = lastUpdated.strftime('%Y-%m-%dT%H:%M:%S.%f')
            self.metadata['lastUpdated'] = lastUpdated.strftime('%Y-%m-%d %H:%M:%S.000')
        if write:
            self.writeMetadata()

    def setLastUpdatedForFile(self, filename, lastUpdated, *, write=True):
        """
        Set the last updated date for a specific file.
        Adds the key 'files' to the metadata if it does not exist yet.
        Adds the key 'filename' to the 'files' key if it does not exist yet.

        args:
            filename (str): The filename of the file to set the last updated date for
            lastUpdated (str or datetime): The last updated date to set   
        """
        if not 'files' in self.metadata:
            self.metadata['files'] = {}
        if not filename in self.metadata['files']:
            self.metadata['files'][filename] = {}
        if isinstance(lastUpdated, str):
            self.metadata['files'][filename]['lastUpdated'] = lastUpdated
        elif isinstance(lastUpdated, datetime):
            # self.metadata['files'][filename] = lastUpdated.strftime('%Y-%m-%dT%H:%M:%S.%f')
            self.metadata['files'][filename] = lastUpdated.strftime('%Y-%m-%d %H:%M:%S.000')
        if write:
            self.writeMetadata()
    def setLatestImageDownloadUrlForFile(self, filename, latestImageDownloadUrl, *, write=True):
        """
        Set the latest image download url for a specific file.
        Adds the key 'files' to the metadata if it does not exist yet.
        Adds the key 'filename' to the 'files' key if it does not exist yet.

        args:
            filename (str): The filename of the file to set the last updated date for
            latestImageDownloadUrl (str): The latest image download url to set
        """
        if not 'files' in self.metadata:
            self.metadata['files'] = {}
        if not filename in self.metadata['files']:
            self.metadata['files'][filename] = {}
        if isinstance(latestImageDownloadUrl, str):
            self.metadata['files'][filename]['latestImageDownloadUrl'] = latestImageDownloadUrl
        if write:
            self.writeMetadata()
            
    def setLatestPdfDownloadUrlForFile(self, filename, latestPdfDownloadUrl, *, write=True):
        """
        Set the latest pdf download url for a specific file.
        Adds the key 'files' to the metadata if it does not exist yet.
        Adds the key 'filename' to the 'files' key if it does not exist yet.

        args:
            filename (str): The filename of the file to set the last updated date for
            latestPdfDownloadUrl (str): The latest image download url to set
        """
        if not 'files' in self.metadata:
            self.metadata['files'] = {}
        if not filename in self.metadata['files']:
            self.metadata['files'][filename] = {}
        if isinstance(latestPdfDownloadUrl, str):
            self.metadata['files'][filename]['latestPdfDownloadUrl'] = latestPdfDownloadUrl
        if write:
            self.writeMetadata()

    def setKeyValueForFile(self, filename, key, value, *, write=True):
        """
        Set a metadata key for a specific file to a given value.
        The key for the specific file must exist already:

        args:
            filename (str): The filename of the file to set the last updated date for
            key (str): The key to set
            value (str): The value to set
            write (bool, optional): Whether to write the metadata to the metadata file. Defaults to True.
        """
        self.metadata['files'][filename][key] = value
        if write:
            self.writeMetadata()

    def writeMetadata(self):
        """
        Writes the metadata to the metadata file.
        """
        with open(self.metadataFile, 'w') as f:
            json.dump(self.metadata, f, indent=4)