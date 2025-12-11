"""
Utils file with classes and functions to manage interactions with an easydb instance (part of the code comes from https://docs.easydb.de/en/technical/)
"""

import time
import json
import os
import copy
import argparse
import pytz
import requests, zipfile, io
import xml.etree.ElementTree as ET
from datetime import datetime
from tqdm import tqdm
from utils.logger_helper import setup_logger
logger = setup_logger()

APPROVED_TAG = 207
    
class Export:
    """
    Export class handles constructing export objects and JSON files
    """
    APPROVED_TAG = 12
    
    #template for search using objecttype
    objecttype_search_template = {
                "type": "in",
                "bool": "must",
                "fields": [
                    "_objecttype"
                ],
                "in": None
            }
    
    #template for search using tags  
    tags_search_template = {
                "type": "in",
                "bool": "must",
                "fields": [
                    "_tags._id"
                ],
                "in": None
            }
    
    #template for search using pools
    pools_search_template = {
                'bool': 'should',
                'type': 'in',
                'fields': None,
                'in': None
            }
    
    # template for search for a changelog date
    changelog_search_template = {
        'type': 'changelog_range',
        'from': None,
        'operation': None,
        'user': None
    }
    
    
    #template for export object
    export_dict_template = {
       'export': {
           '_version': 1,
            'type': 'export',
            'search': {
                "search": None,
                "format": "long",
                "objecttypes": None,
                "limit": None
            },
            'fields': None,
            'classes': {},
            'assets': {},
            'csv': False,
            'name': None,
            'xml': True,
            'xml_one_file_per_object': True,
            'merge_linked_objects': 'none',
            'merge_max_depth': 1,
            'json': False,
            'json_one_file_per_object': False,
            'all_languages': False,
            'mapping': 'easydb',
            'flat': False,
            'batch_size': 5000,
            'filename_template': None,
            'eas_fields': {},
            'produce_options': {
                'addLinkedData': True, 
                'plugin': 'easydb'
            },
            'limit': None,
            'offset': None
        }
    }
    
    _exportid = None
    
    def __init__(self, object_type, isSample):
        if object_type == 'person':
            self.name = 'ETL Process Person [Production]'
            self.tags = [88]
            self.objecttypes = ['act_grpm_0103']
            self.pool_fields = ['act_grpm_0103._pool.pool._id']
            self.pool_ids = [124] if isSample else [85, 108]

        elif object_type == 'group':
            self.name = 'ETL Process Group [Production]'
            self.tags = [89, 90]
            self.objecttypes = ['act_grpm_0103']
            self.pool_fields = ['act_grpm_0103._pool.pool._id']
            self.pool_ids = [124] if isSample else [85, 108]
            
        elif object_type == 'architectural_competition':
            self.name = 'ETL Process Architectural Competition [Production]'
            self.tags = [210] if isSample else None
            self.objecttypes = ['ac']
            self.pool_fields = ['ac._pool.pool._id']            
            self.pool_ids = None
            
        elif object_type == 'archival_object':
            self.name = 'ETL Process Archival Unit [Production]'
            self.tags = [APPROVED_TAG]
            self.objecttypes = ['au_grpm_16']
            self.pool_fields = ['au_grpm_16._pool.pool._id']
            self.pool_ids = [122] if isSample else [17, 127]
            
        elif object_type == 'bibliographic_item':
            self.name = 'ETL Process Bibliographic Item [Production]'
            self.tags = None
            self.objecttypes = ['bi_grpm_08']
            self.pool_fields = ['bi_grpm_08._pool.pool._id']
            self.pool_ids = [119] if isSample else [92]
            
        elif object_type == 'digital_object':
            self.name = 'ETL Process Digital Object [Production]'
            self.tags = None
            self.objecttypes = ['do_grpm_06']
            self.pool_fields = ['do_grpm_06._pool.pool._id']
            self.pool_ids = [125] if isSample else [26, 59]
            
        elif object_type == 'oeuvre':
        # elif object_type == 'oeu':
            self.name = 'ETL Process Œuvre [Production]'
            self.tags = [91]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [119] if isSample else [92]
            
        elif object_type == 'built_work':
            self.name = 'ETL Process Built Work [Production]'
            self.tags = [92]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [119] if isSample else [92]
            
        elif object_type == 'project':
            self.name = 'ETL Process Architectural Project [Production]'
            self.tags = [93]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [119] if isSample else [92]
            
        elif object_type == 'place':
            self.name = 'ETL Process Place [Production]'
            self.tags = None
            self.objecttypes = ['pl_grpm_05']
            self.pool_fields = ['pl_grpm_05._pool.pool._id']
            self.pool_ids = [128] if isSample else None
            
        else:
            logger.warning('Unsupported input')

    def _setId(self, id=None):
        self._id = id

    def _getId(self):
        return self._id
    
    id = property(_getId, _setId)


def getExportDict(export, isSample, object_ids=None):
    
    search_list = []
     
    objecttype_search = export.objecttype_search_template
    objecttype_search['in'] = export.objecttypes
    search_list.append(objecttype_search)
    
    if export.pool_ids is not None:
        pools_search = export.pools_search_template
        pools_search['fields'] = export.pool_fields
        pools_search['in'] = export.pool_ids
        search_list.append(pools_search)

    if export.tags is not None: 
        tags_search = export.tags_search_template
        tags_search['in'] = export.tags
        search_list.append(tags_search)
        
        # object_ids filter (chunking)
    if object_ids is not None and len(object_ids) > 0:
        object_ids_search = {"type": "in", "bool": "must", "fields": ["_system_object_id"], "in": object_ids}
        search_list.append(object_ids_search)
        
    export_dict = export.export_dict_template
    export_dict['export']['name'] = export.name
    export_dict['export']['search']['objecttypes'] = export.objecttypes
    export_dict['export']['search']['search'] = search_list
    
    return export_dict


"""
Session class handles all Session API applications
"""


class Session:
    _session, _token, _header, _content, _plugins, _password, _login = "", "", "", "", "", "", ""
    
    def __init__(self, server):
        http = "http://"
        if server.startswith("http"):
            http = ""

        self.new_session = http + server + "/api/v1/session"
        self.auth_session = http + server + "/api/v1/session/authenticate"
        self.deauth_session = http + server + "/api/v1/session/deauthenticate"
        self.search = http + server + "/api/v1/search"
        self.export = http + server + "/api/v1/export"
        self.plugin = http + server + "/api/v1/plugin"
        self.server = http + server + "/api/v1/plugin/base/server/status"

    def _setSession(self, session=None):
        self._session = session

    def _getSession(self):
        return self._session

    def _setHeader(self, header):
        self._header = header

    def _getHeader(self):
        return self._header

    def _setToken(self, token):
        self._token = token

    def _getToken(self):
        return self._token

    def _setContent(self, content):
        self._content = content

    def _getContent(self):
        return self._content

    def _setPassword(self, password):
        self._password = password

    def _getPassword(self):
        return self._password

    def _setLogin(self, login):
        self._login = login

    def _getLogin(self):
        return self._login

    def _setPlugins(self, plugins):
        self._plugins = plugins

    def _getPlugins(self):
        return self._plugins

    token = property(_getToken, _setToken)
    header = property(_getHeader, _setHeader)
    session = property(_getSession, _setSession)
    content = property(_getContent, _setContent)
    password = property(_getPassword, _setPassword)
    login = property(_getLogin, _setLogin)
    plugins = property(_getPlugins, _setPlugins)



def check_for_updates(ezdb, objecttype, lastUpdated):
    """
    Check if there are any updated objects since the last download.
    """
    headers = {
        "X-EasyDB-Token": ezdb.token
    }
    
    if objecttype == 'person':
        objecttypes = ['act_grpm_0103']
        tags = [88]
    elif objecttype == 'group':
        objecttypes = ['act_grpm_0103']
        tags = [89, 90]
    elif objecttype == 'architectural_competition':
        objecttypes = ['ac']
        tags = None
    elif objecttype == 'archival_object':
        objecttypes = ['au_grpm_16']
        tags = [APPROVED_TAG]
    elif objecttype == 'bibliographic_item':
        objecttypes = ['bi_grpm_08']
        tags = None
    elif objecttype == 'digital_object':
        objecttypes = ['do_grpm_06']
        tags = None
    elif objecttype == 'oeuvre':
        objecttypes = ['oeu']
        tags = [91]
    elif objecttype == 'built_work':
        objecttypes = ['oeu']
        tags = [92]
    elif objecttype == 'project':
        objecttypes = ['oeu']
        tags = [93]
    elif objecttype == 'place':
        objecttypes = ['pl_grpm_05']
        tags = None
    else:
    # print('Unsupported input')
        logger.warning('Unsupported input')
    

    payload = {
        "format": "standard",
        "type": "object",
        # differentiate
        "tags": tags,
        "objecttypes": objecttypes,
        "language": "de-DE",
        "limit": 1,
        "offset": 0,
        "sort": [{"field": "_last_modified", "order": "DESC"}]
    }

    response = requests.post(ezdb.search, json=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        updated_objects = data.get("objects", [])
        lastUpdatedServer = updated_objects[0]["_last_modified"]
                
        if lastUpdatedServer > lastUpdated:
            logger.info("There are modifications since the last download. Creating new export...")
            return True
    else:
        logger.error(f"Failed to fetch data: {response.status_code}")
        logger.error(response.text)
        print(response.text)
    return False    


"""
Create new session using URL directed towards database
"""


def start_session(ezdb):
    try:
        logger.info("start session")
        r = requests.get(ezdb.new_session)
        check_status_code(r, True)
    except requests.exceptions.ConnectionError as e:
        server_url_error_message(ezdb.new_session, e)

    ezdb.session = r
    ezdb.header = r.headers

    ezdb.token = getVal(r.json(), "token")
    ezdb.content = r.json()


"""
Retrieve the same session using Token and plain url
Compare instances to prove similarity
"""


def retrieve_current_session(ezdb):
    payload = {
        "token": ezdb.token
    }

    logger.info("retrieve current session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.get(ezdb.new_session, params=payload)
    check_status_code(r, True)

    # proof that the session is the same
    if getVal(r.json(), "instance") == getVal(ezdb.content, "instance"):
        logger.info("retrieved correct session")


"""
Authenticate Session using authenticate url
login and password credentials required, or email instead of login
"""


def authenticate_session(ezdb):
    ezdb.login = ezdb.login
    ezdb.password = ezdb.password

    payload = {
        "token": ezdb.token,
        "login": ezdb.login,
        "password": ezdb.password
    }

    logger.info("authenticate session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.post(ezdb.auth_session, params=payload)
    check_status_code(r, True)


"""
Deauthenticate session using deauthenticate url
"""


def deauthenticate_session(ezdb):
    payload = {
        "token": ezdb.token
    }

    logger.info("deauthenticate session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.post(ezdb.deauth_session, params=payload)
    check_status_code(r)

    
"""
Functions for managing exports
"""

# check if export of a content type already exists, if yes, delete it
def check_purge_export(ezdb):
    tokenpayload = {
        "token": ezdb.token
    }

    r = requests.get(ezdb.export, params=tokenpayload)
    json_data = json.loads(r.content.decode('utf-8'))
    
    if (json_data['count'] > 0):
        logger.info(f"Number of existing exports: {json_data['count']}")    
        for ob in json_data['objects']:
            export_id = ob['export']['_id']
            requests.delete(ezdb.export + "/" + str(export_id), params=tokenpayload)
        logger.info('Exports deleted')
    else:
        logger.info('No existing exports')
    return 0

def get_object_ids_for_pools(ezdb, objecttype, pool_ids, page_size=1000):
    """
    Return a list of internal object _id values for objects of `objecttype`
    that are members of the pools in `pool_ids`.
    """
    headers = {
        "X-EasyDB-Token": ezdb.token
    }
    object_ids = []
    offset = 0

    # build a search payload, filter by objecttype and pools
    while True:
        payload = {
            "format": "standard",
            "type": "object",
            "objecttypes": [objecttype],
            "language": "de-DE",
            "limit": page_size,
            "offset": offset,
            "search": [
                {"type": "in", "bool": "must", "fields": ["_objecttype"], "in": [objecttype]},
                {"bool": "should", "type": "in", "fields": [f"{objecttype}._pool.pool._id"], "in": pool_ids}
            ]
        }

        try:
            r = requests.post(ezdb.search, json=payload, headers=headers, timeout=60)
            check_status_code(r)
            data = r.json()
        except Exception:
            logger.exception("Failed to fetch object ids for pools %s (offset %d)", pool_ids, offset)
            break

        objs = data.get("objects", [])
        if not objs:
            break

        for o in objs:
            if "_system_object_id" in o:
                object_ids.append(int(o["_system_object_id"]))
            elif "object" in o and "_system_object_id" in o["object"]:
                object_ids.append(int(o["object"]["_system_object_id"]))
            else:
                logger.debug("Object without _system_object_id encountered: %s", o)

        if len(objs) < page_size:
            break
        offset += page_size

    logger.info("Found %d object ids for pools %s", len(object_ids), pool_ids)
    return object_ids


def create_export(ezdb, type_, pools, isSample, object_id_subset=None):
    logger.info('Creating export for {} object type'.format(type_))
    tokenpayload = {
        "token": ezdb.token
    }    
    export_object = Export(type_, pools)
    export_dict = getExportDict(export_object, isSample, object_id_subset)
    r = requests.put(ezdb.export, params=tokenpayload, data=json.dumps(export_dict))
    check_status_code(r)
    export_creation_json = r.json()
    if 'code' in export_creation_json.keys():
        logger.error('Could not create export: {}'.format(export_creation_json['code']))
        return None, None
    export_object._setId(export_creation_json['export']['_id'])
    return export_object, export_creation_json


def start_export(ezdb, export_object):
    logger.info('Initiating export')
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r = requests.post(ezdb.export + '/' + str(export_id) + '/start', params=tokenpayload)
    check_status_code(r)
    return export_object, r.json()


def check_export_status(ezdb, export_object):
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r = requests.get(ezdb.export + '/' + str(export_id), params=tokenpayload)
    check_status_code(r)
    return r.json()


def download_export(ezdb, export_object, local_path, metadata_obj, filenamePrefix):
    logger.info('Downloading export')
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r =  requests.get(ezdb.export + '/{}/zip'.format(export_id), params=tokenpayload)
    check_status_code(r)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    
    namespace = {'ns': 'https://schema.easydb.de/EASYDB/1.0/objects/'}
    
    files_to_download_list = list(enumerate(z.namelist()))
    
    for i, item in tqdm(files_to_download_list, desc=f"Downloading {export_object.objecttypes} files", total=len(files_to_download_list), unit="file"):
        
        filename = os.path.basename(item)
        # skip directories
        if not filename:
            continue

        source = z.open(item)
        xml_content = source.read()
        
        tree = ET.ElementTree(ET.fromstring(xml_content))
        root = tree.getroot()
        
        lastModified = root.find('.//ns:_last_modified', namespaces=namespace)
        _system_object_id = root.find('.//ns:_system_object_id', namespaces=namespace)
        
        if lastModified is not None:
            lastModifiedText = lastModified.text
        else:
            lastModifiedText = None
            
        if _system_object_id is not None:
            _system_object_idText = _system_object_id.text
        else:
            _system_object_idText = None
            
        filename = f"{filenamePrefix}{_system_object_idText}.xml"
        
         
        with open(os.path.join(local_path, filename), "wb") as target:
            target.write(xml_content)
            
        metadata_obj.setLastUpdatedForFile(filename, lastModifiedText, write=False)
        
    # Update metadata with the current download start time
    newlastUpdatedModule = datetime.now(pytz.utc)
    logger.info(f'New last update module: {newlastUpdatedModule}')
    metadata_obj.setLastUpdated(newlastUpdatedModule)
        
    logger.info('Successfully saved files to {}.'.format(local_path))
    return 0


def delete_export(ezdb, export_object):
    logger.info('Cleaning up (deleting remote export)')
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r = requests.delete(ezdb.export + '/' + str(export_id), params=tokenpayload)
    check_status_code(r)
    return 0
  

def run_export_pipeline(ezdb, objecttype, path, metadata_obj, filenamePrefix, pools=None, isSample=False, pool_chunk_size=30000):
    check_purge_export(ezdb)

    # If pools: expand them to object ids and chunk those ids.
    if pools:
        logger.info(f"Running export for pools: {pools}")
        object_ids = get_object_ids_for_pools(ezdb, objecttype, pools, page_size=1000)
        if not object_ids:
            logger.error("No object ids found for pools %s — aborting export run", pools)
            return

        # chunk object ids into exports of size `pool_chunk_size`
        object_id_chunks = [object_ids[i:i + pool_chunk_size] for i in range(0, len(object_ids), pool_chunk_size)]
    else:
        object_id_chunks = [None]

    # process each object-id chunk 
    for idx, object_id_subset in enumerate(object_id_chunks, start=1):
        size = len(object_id_subset) if object_id_subset else 'ALL'
        logger.info(f"Running object-id chunk {idx}/{len(object_id_chunks)} (size: {size})")

        # create export with these object ids (or full export if None)
        export_object, _ = create_export(ezdb, objecttype, pools, isSample, object_id_subset)
        if not export_object:
            logger.error("Export creation failed for chunk %d — skipping", idx)
            continue

        # start export
        try:
            export_object, _ = start_export(ezdb, export_object)
        except Exception:
            logger.exception("start_export failed for chunk %d — attempting cleanup and skipping", idx)
            try:
                delete_export(ezdb, export_object)
            except Exception:
                logger.exception("delete_export failed during cleanup for chunk %d", idx)
            continue

        curr_state = 'processing'
        logger.info('Processing export')
        while True:
            try:
                export_check_json = check_export_status(ezdb, export_object)
            except Exception:
                logger.exception("check_export_status raised an exception; will retry shortly")
                time.sleep(2)
                continue

            curr_state = export_check_json.get('_state')
            logger.info(f'Export state: {curr_state}')
            if curr_state == 'done':
                logger.info("done")
                break
            if curr_state == 'failed':
                logger.error("Export failed")
                break
            time.sleep(2)

        if curr_state == 'done':
            try:
                download_export(ezdb, export_object, path, metadata_obj, filenamePrefix)
            except Exception:
                logger.exception("download_export failed for chunk %d", idx)
        else:
            logger.error("Export failed or no records found for chunk %d", idx)

        try:
            delete_export(ezdb, export_object)
        except Exception:
            logger.exception("delete_export failed for chunk %d", idx)
    
    
def perform_curl_request(req):
    command = "curl -X {method} -H {headers} -d '{data}' '{uri}'"
    method = req.method
    uri = req.url
    data = req.body
    headers = ['"{0}: {1}"'.format(k, v) for k, v in req.headers.items()]
    headers = " -H ".join(headers)
    return command.format(method=method, headers=headers, data=data, uri=uri)


"""
Print the Root Menu About
"""


def root_menu_about(ezdb):
    aboutDetails = {
        "api": "",
        "server_version": "",
        "user-schema": "",
        "solution": "",
        "instance": "",
        "db-name": "",
        "Plugins": "",
        "Optionen": "",
        "last-modified": "",
        "Fivegit": "",
        "CUIgit": "",
        "Style": "",
        "server": ""
    }

    logger.info(ezdb.header)

    instance = getVal(ezdb.content, "instance")
    for key, value in instance.items():
        if key in aboutDetails:
            aboutDetails[key] = value

        # Instance code is labelled as 'name' in dict
        if key == "name":
            aboutDetails["instance"] = value

    for key, value in ezdb.header.items():
        if key in aboutDetails:
            aboutDetails[key] = value

    # Get Plugins
    logger.info("get plugins")
    r = requests.get(ezdb.plugin)
    check_status_code(r)
    ezdb.plugins = r.json()["plugins"]

    plgns = []
    for plg in ezdb.plugins:
        plgns.append(plg["name"])

    aboutDetails["Plugins"] = plgns

    # Get Server Info
    payload = {
        "token": ezdb.token
    }
    logger.info("get server info")
    r = requests.get(ezdb.server, params=payload)
    check_status_code(r)

    pretty_printer(aboutDetails)


"""
Helper Methods
"""


def getVal(data, str):
    for key, value in data.items():
        if key == str:
            return value


def write_json(data, name):
    with open(name, "w") as outfile:
        json.dump(data, outfile, indent=4)


def write_file(self, r, filename):
    with open(filename, "wb") as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)


def pretty_printer(dict):

    print("{:<20} {:<20}".format("About", "Information"))
    for k, v in dict.items():
        if v == "":
            continue
        if isinstance(v, list):
            print("{:<20} {:<20}".format(k, ", ".join(v)))
            continue

        print("{:<20} {:<20}".format(k, v))


def check_status_code(response, exit_on_failure=False):
    if response.status_code != 200:
        logger.info("got status code %s: %s" %
              (response.status_code, json.dumps(response.json(), indent=4)))
        if exit_on_failure:
            logger.error("exit after unexpected status code")
            print("exit after unexpected status code")
            exit(1)


"""
error_message
"""

def server_url_error_message(str, err):
    logger.error("URL is invalid")
    print("URL is invalid")
    logger.error("{0} raises {1}").format(str, err)
    print("{0} raises {1}").format(str, err)
    # sys.exit()
    exit(1)

