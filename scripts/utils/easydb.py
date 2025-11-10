"""
Utils file with classes and functions to manage interactions with an easydb instance (part of the code comes from https://docs.easydb.de/en/technical/)
"""

import requests
import time
import json
import os
import copy
import argparse
import requests, zipfile, io
import xml.etree.ElementTree as ET
from datetime import datetime

"""
Export class handles constructing export objects and JSON files
"""

class Export:
    APPROVED_TAG = 12
    # APPROVED_TAG = 207
    
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
            'limit': None
        }
    }
    
    _exportid = None
    
    def __init__(self, object_type, changelog_timestamp, limit):
        if object_type == 'person':
            self.name = 'ETL Process Person [Production]'
            self.tags = [88]
            self.objecttypes = ['act_grpm_0103']
            self.pool_fields = ['act_grpm_0103._pool.pool._id']
            self.pool_ids = [85, 108]
            
        elif object_type == 'group':
            self.name = 'ETL Process Group [Production]'
            self.tags = [89, 90]
            self.objecttypes = ['act_grpm_0103']
            self.pool_fields = ['act_grpm_0103._pool.pool._id']
            self.pool_ids = [85, 108]
            
        elif object_type == 'architectural_competition':
            self.name = 'ETL Process Architectural Competition [Production]'
            self.tags = None
            self.objecttypes = ['ac']
            self.pool_fields = ['ac._pool.pool._id']            
            self.pool_ids = None
            
        elif object_type == 'archival_object':
            self.name = 'ETL Process Archival Unit [Production]'
            self.tags = [self.APPROVED_TAG]
            self.objecttypes = ['au_grpm_16']
            self.pool_fields = ['au_grpm_16._pool.pool._id']
            self.pool_ids = [17, 127]
            
        elif object_type == 'bibliographic_item':
            self.name = 'ETL Process Bibliographic Item [Production]'
            self.tags = None
            self.objecttypes = ['bi_grpm_08']
            self.pool_fields = ['bi_grpm_08._pool.pool._id']
            self.pool_ids = [92]
            
        elif object_type == 'digital_object':
            self.name = 'ETL Process Digital Object [Production]'
            self.tags = None
            self.objecttypes = ['do_grpm_06']
            self.pool_fields = ['do_grpm_06._pool.pool._id']
            self.pool_ids = [26, 59]
            
        elif object_type == 'oeuvre':
            self.name = 'ETL Process Œuvre [Production]'
            self.tags = [91]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [92]
            
        elif object_type == 'built_work':
            self.name = 'ETL Process Built Work [Production]'
            self.tags = [92]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [92]
            
        elif object_type == 'project':
            self.name = 'ETL Process Architectural Project [Production]'
            self.tags = [93]
            self.objecttypes = ['oeu']
            self.pool_fields = ['oeu._pool.pool._id']
            self.pool_ids = [92]
            
        elif object_type == 'place':
            self.name = 'ETL Process Place [Production]'
            self.tags = None
            self.objecttypes = ['pl_grpm_05']
            self.pool_fields = ['pl_grpm_05._pool.pool._id']
            self.pool_ids = None
            
        else:
            print('Unsupported input')
            
        self.changelog_timestamp = changelog_timestamp
        self.limit = limit

    def _setId(self, id=None):
        self._id = id

    def _getId(self):
        return self._id
    
    id = property(_getId, _setId)


def getExportDict(export):
    
    search_list = []
    
    # search only for records that have been changed after the given date
    changelog_search = export.changelog_search_template
    changelog_search['from'] = export.changelog_timestamp
    search_list.append(changelog_search)
     
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
        
    export_dict = export.export_dict_template
    export_dict['export']['name'] = export.name
    export_dict['export']['search']['objecttypes'] = export.objecttypes
    export_dict['export']['search']['search'] = search_list
    export_dict['export']['search']['limit'] = export.limit
    
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


"""
Create new session using URL directed towards database
"""


def start_session(ezdb):
    try:
        print("start session")
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

    print("retrieve current session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.get(ezdb.new_session, params=payload)
    check_status_code(r, True)

    # proof that the session is the same
    if getVal(r.json(), "instance") == getVal(ezdb.content, "instance"):
        print("retrieved correct session")


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

    print("authenticate session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.post(ezdb.auth_session, params=payload)
    check_status_code(r, True)


"""
Deauthenticate session using deauthenticate url
"""


def deauthenticate_session(ezdb):
    payload = {
        "token": ezdb.token
    }

    print("deauthenticate session, payload: %s" % json.dumps(payload, indent=4))
    r = requests.post(ezdb.deauth_session, params=payload)
    check_status_code(r)

    
"""
Functions for managing exports
"""

# check if export of a content type already exists, if yes, delete it
def check_purge_export(ezdb):
    print("Checking existing exports...")
    tokenpayload = {
        "token": ezdb.token
    }

    r = requests.get('https://collections.gta.arch.ethz.ch/api/v1/export', params=tokenpayload)
    json_data = json.loads(r.content.decode('utf-8'))
    
    if (json_data['count'] > 0):
        print(f"Number of existing exports: {json_data['count']}")    
        for ob in json_data['objects']:
            export_id = ob['export']['_id']
            print(export_id)
            requests.delete('https://collections.gta.arch.ethz.ch/api/v1/export/' + str(export_id), params=tokenpayload)
        print('Exports deleted')
    else:
        print('No existing exports')
    return 0

def create_export(ezdb, type_, changelog_timestamp, limit):
    print('Creating export for {} object type'.format(type_))
    tokenpayload = {
        "token": ezdb.token
    }
    export_object = Export(type_, changelog_timestamp, limit)
    export_dict = getExportDict(export_object)
    r = requests.put(ezdb.export, params=tokenpayload, data=json.dumps(export_dict))
    check_status_code(r)
    export_creation_json = r.json()
    if 'code' in export_creation_json.keys():
        print('Could not create export: {}'.format(export_creation_json['code']))
        return None, None
    export_object._setId(export_creation_json['export']['_id'])
    return export_object, export_creation_json


def start_export(ezdb, export_object):
    print('Initiating export')
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
    print('Downloading export')
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r =  requests.get('https://collections.gta.arch.ethz.ch/api/v1/export/{}/zip'.format(export_id), params=tokenpayload)
    check_status_code(r)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    
    namespace = {'ns': 'https://schema.easydb.de/EASYDB/1.0/objects/'}
    
    for i, item in enumerate(z.namelist()):
        # if i > 50:
        #     continue
        
        filename = os.path.basename(item)
        # skip directories
        if not filename:
            continue

        source = z.open(item)
        xml_content = source.read()
        
        tree = ET.ElementTree(ET.fromstring(xml_content))
        root = tree.getroot()
        
        lastModified = root.find('.//ns:_last_modified', namespaces=namespace)
        # uuid = root.find('.//ns:_uuid', namespaces=namespace)
        _system_object_id = root.find('.//ns:_system_object_id', namespaces=namespace)
        
        if lastModified is not None:
            dt = datetime.strptime(lastModified.text, '%Y-%m-%dT%H:%M:%SZ')
            lastModifiedText = dt.strftime('%Y-%m-%d %H:%M:%S.000')
 
        else:
            lastModifiedText = None
        
        # if uuid is not None:
        #     uuidText = uuid.text
        # else:
        #     uuidText = None
        if _system_object_id is not None:
            _system_object_idText = _system_object_id.text
        else:
            _system_object_idText = None
            
        filename = f"{filenamePrefix}{_system_object_idText}.xml"
        
        # print(f'filename: {filename}')
        print(f'fullpath: {local_path}{filename}')
         
        with open(os.path.join(local_path, filename), "wb") as target:
            target.write(xml_content)
            
        metadata_obj.setLastUpdatedForFile(filename, lastModifiedText, write=False)
        
    print('Successfully saved files to {}.'.format(local_path))
    return 0


def delete_export(ezdb, export_object):
    print('Cleaning up (deleting remote export)')
    tokenpayload = {
        "token": ezdb.token
    }
    export_id = export_object.id
    r = requests.delete('https://collections.gta.arch.ethz.ch/api/v1/export/' + str(export_id), params=tokenpayload)
    check_status_code(r)
    return 0
  

def run_export_pipeline(ezdb, objecttype, changelog_timestamp, path, limit, metadata_obj, filenamePrefix):
    check_purge_export(ezdb)
    export_object, export_creation_json = create_export(ezdb, objecttype, changelog_timestamp, limit)
    export_object, export_start_json= start_export(ezdb, export_object)
    curr_state = 'processing'
    print('Processing export')
    while True:
        print(f'Export state: {curr_state}')
        export_check_json = check_export_status(ezdb, export_object)
        curr_state = export_check_json['_state']
        if curr_state in ['done']:
            print("done")
            break
        if curr_state in ['failed']:
            print('failed')
            break
        time.sleep(8)
    
    if curr_state == 'done':
        download_export(ezdb, export_object, path, metadata_obj, filenamePrefix)
    else:
        print('Export failed or no records that match the search query were found')
    return delete_export(ezdb, export_object)
    
    
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

    print(ezdb.header)

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
    print("get plugins")
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
    print("get server info")
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
        print("got status code %s: %s" %
              (response.status_code, json.dumps(response.json(), indent=4)))
        if exit_on_failure:
            print("exit after unexpected status code")
            exit(1)


"""
error_message
"""

def server_url_error_message(str, err):
    print("URL is invalid")
    print("{0} raises {1}").format(str, err)
    # sys.exit()
    exit(1)

