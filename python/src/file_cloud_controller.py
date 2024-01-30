import zipfile
from enum import Enum
from http import client
from typing import Dict, List
from xml.dom.minidom import parseString
from xml.etree import ElementTree

import requests
from pandas.io import json

from .utils import bcolors, time, timerdecorator


def dict_to_params(dic: Dict):
    return "&".join([f"{key}={value}" for (key, value) in dic.items()])


class MetadataSets(Enum):
    TEST = "64c75ff4a051780403092b9c"


class FileCloudController:
    def __init__(self, server_url, user, passwd):
        self.server_url = server_url
        self.user = user
        self.passwd = passwd
        self.session = requests.session()
        self.admin_session = requests.session()

    # Should I return session or just add session to self
    def admin_login(self):
        """Connects to File Cloud server as admin"""
        print(
            bcolors.OKCYAN + f"Logging into FileCloud Server (admin)..." + bcolors.ENDC
        )
        login_endpoint = "/admin/adminlogin"
        credentials = {"adminuser": self.user, "adminpassword": self.passwd}
        headers = {"Accept": "application/json"}
        login_call = self.admin_session.post(
            self.server_url + login_endpoint,
            data=credentials,
            headers=headers,
        )
        try:
            login_call = login_call.json()
            if login_call["command"][0]["result"] == 1:
                print(bcolors.OKCYAN + f"Login Succeeded!" + bcolors.ENDC)
            else:
                print(bcolors.FAIL + f"Login Failed" + bcolors.ENDC, end=" ")
                raise ValueError(login_call["command"][0]["message"])
        except requests.exceptions.JSONDecodeError as e:
            result = ElementTree.fromstring(login_call.text).find(".//result")

            if int(result.text) > 0:
                print(bcolors.OKCYAN + f"Login Succeeded!" + bcolors.ENDC)
            else:
                print(bcolors.FAIL + f"Login Failed" + bcolors.ENDC)

    def login(self):
        """Connects to File Cloud server"""
        print(
            bcolors.OKCYAN + f"Logging into FileCloud Server (core)..." + bcolors.ENDC,
        )
        login_endpoint = "/core/loginguest"
        credentials = {"userid": self.user, "password": self.passwd}
        headers = {
            "Accept": "application/json",
            "User-agent": "Mozilla/5.0",
        }
        login_call = self.session.post(
            self.server_url + login_endpoint,
            data=credentials,
            headers=headers,
            allow_redirects=True,
        )
        self.cookies = login_call.cookies
        self.headers = login_call.headers["Set-Cookie"]
        """
        params = "&".join([f"{key}={value}" for (key, value) in credentials.items()])
        login_call = self.session.get(
            self.server_url + login_endpoint + f"?{params}",
            data=credentials,
            headers=headers,
        ).json()
        """
        try:
            login_call = login_call.json()

            if login_call["command"][0]["result"] == 1:
                print(bcolors.OKCYAN + f"Login Succeeded!" + bcolors.ENDC)
            else:
                print(bcolors.FAIL + f"Login Failed" + bcolors.ENDC, end=" ")
                raise ValueError(login_call["command"][0]["message"])
        except requests.exceptions.JSONDecodeError as e:
            result = ElementTree.fromstring(login_call.text).find(".//result")

            if int(result.text) > 0:
                print(bcolors.OKCYAN + f"Login Succeeded!" + bcolors.ENDC)
            else:
                print(bcolors.FAIL + f"Login Failed" + bcolors.ENDC)

    def _metadata_params(self, path: str, kvp: Dict, metadata: Dict) -> Dict:
        params = {}
        params["fullpath"] = path
        params["setid"] = metadata["setid"]
        params["attributes_total"] = metadata["total"]

        for key, value in list(metadata.items())[2:]:
            params[f"{key}_attributeid"] = value["id"]
            params[f"{key}_value"] = kvp[value["name"]]

        return params

    def _create_upload_api_params(self, path: str, filename) -> Dict:
        params = {
            "appname": "explorer",
            "path": path,
            "offset": 0,
        }
        return params

    def _set_metadata_params(self, fullpath, setid):
        params = {"fullpath": fullpath, "setid": setid}
        return params

    def _create_folder_params(self, path, name):
        params = {"name": name, "path": path}
        return params

    def list_dirs(self):
        get_file_endpoint = "/core/getfilelist"
        upload_call = self.session.post(
            self.server_url + get_file_endpoint,
            cookies=self.cookies,
        )

        for child in ElementTree.fromstring(upload_call.text).iter("path"):
            print(child.text)

    def _get_metadata_defaults(self, setid) -> Dict:
        """Retrieves metadata attribute values to use for adding metadata to files
        using core/getdefaulttmetadatavalues api. API returns XML text containing
        needed attributes

        Parameters
        ----------
            self
                self
            setid
                ID used to identify metadata set in File cloud

        Returns
        -------
            Dict
                <attributen>:
                    Id  :   <attributen_name>
                    name:   <arrtibuten_setid>
        """
        metadata_attributes = {}
        metadata_endpoint = f"/core/getdefaultmetadatavalues?setid={setid}"
        response = self.session.post(
            self.server_url + metadata_endpoint,
        )
        # Gets all the child node of <metadatasetvalue>
        _attributes = ElementTree.fromstring(response.text).findall(
            "metadatasetvalue/*"
        )

        # Gets all the nodes which contain the word "attribute"
        attributes = [
            attribute
            for attribute in _attributes
            if attribute.tag.__contains__("attribute")
        ]
        metadata_attributes["setid"] = setid
        metadata_attributes["total"] = attributes[-1].text

        # Gets attributeid and attribute name
        for index, element in enumerate(attributes[:-1:7]):
            metadata_attributes[element.tag.split("_")[0]] = {
                "id": element.text,
                "name": attributes[index + 1].text,
            }

        return metadata_attributes

    def _create_metadata(
        self,
        metadata_name,
        kvp: Dict,
    ):
        metadatalist_endpoint = "/admin/addmetadataset"
        data = {
            "name": metadata_name,
            "description": "KVP thingy for thingy",
            "type": "3",
            "disabled": "false",
            "allowallpaths": "true",
            "attributes_total": len(kvp),
            "user0_name": self.user,
            "user0_read": "true",
            "user0_write": "true",
            "users_total": "1",
            "groups_total": "0",
            "paths_total": "0",
        }
        for index, key in enumerate(kvp.keys()):
            data[f"attribute{index}_name"] = key
            data[f"attribute{index}_description"] = ""
            data[f"attribute{index}_type"] = "1"
            data[f"attribute{index}_required"] = "false"
            data[f"attribute{index}_disabled"] = "false"
            data[f"attribute{index}_defaultvalue"] = ""
            data[f"attribute{index}_predefinedvalues_total"] = "0"

        params = "&".join([f"{key}={value}" for (key, value) in data.items()])
        params = dict_to_params(data)
        response = self.admin_session.post(
            self.server_url + metadatalist_endpoint,
            data=data,
        )
        return response

    def _get_available_metadata(self, path, metadata_set) -> Dict:
        """
        Retrieves Metadateset for paired KVP. First gets the metadata that is
        available for the file, then parses the information needed into a dict.
        Returns an empty dict if the needed metadata is not available

        Parameters
        ----------
            path
                path of file to add metadata
            metadata_set
                name of metadataset to add to file

        Returns
        -------
            Dict
                dictionary containing the following:
                    setid
                    attributen ids and name
                these are needed to save the custom metadata to the file

        """
        metadata_attributes = {}

        metadata_endpoint = f"/core/getavailablemetadatasets?fullpath={path}"
        response = self.session.post(f"{self.server_url}{metadata_endpoint}")
        _attributes = []

        # Loops through available metadata to find needed metadata set
        for i in ElementTree.fromstring(response.text).findall(".//metadataset"):
            if (i.find(".//name").text) == metadata_set:
                metadata_attributes["setid"] = i.find(".//id").text
                _attributes.extend(i.findall(".//*"))

        if not _attributes:
            return metadata_attributes

        attributes = [
            attribute
            for attribute in _attributes
            if attribute.tag.__contains__("attribute")
        ]
        metadata_attributes["total"] = attributes[-1].text
        for index, (id, name) in enumerate(zip(attributes[:-1:7], attributes[1:-1:7])):
            metadata_attributes[id.tag.split("_")[0]] = {
                "id": id.text,
                "name": name.text,
            }

        return metadata_attributes

    def _add_metadata(self, path: str, filename: str, kvp):
        """
        Adds Metadata to file specified in parameters. First gets the metadata ids
        then adds the metadata

        Paramters
        ---------
            path
                Path/directory where file is located in filecloud
            filename
                filename
            kvp
                Skriba KVP extracted from zipfile
        """

        # Gets available metadata set based on KVP
        metadata_set_name = f"{kvp['Template']} ({kvp['Page']})"
        available_metadata = self._get_available_metadata(path, metadata_set_name)
        if not available_metadata:
            metadata_creation = self._create_metadata(metadata_set_name, kvp)
            print(metadata_creation.text)
            available_metadata = self._get_available_metadata(path, metadata_set_name)

        data = self._metadata_params(
            path + "/" + filename,
            kvp,
            # self._get_metadata_defaults(metadata_id),
            available_metadata,
        )
        addset_endpoint = "/core/addsettofileobject"

        _addset_params = self._set_metadata_params(data["fullpath"], data["setid"])
        addset_params = dict_to_params(_addset_params)
        addset_response = self.session.post(
            self.server_url + addset_endpoint + f"?{addset_params}"
        )

        print(parseString(addset_response.text).toprettyxml())
        metadata_endpoint = "/core/saveattributevalues"
        saveattribute_response = self.session.post(
            self.server_url + metadata_endpoint,
            data=data,
        )

        print(parseString(saveattribute_response.text).toprettyxml())
        metadata_endpoint = "/core/getmetadatavalues"
        getmetadata_response = self.session.post(
            self.server_url + metadata_endpoint,
            data=data,
        )
        print(parseString(getmetadata_response.text).toprettyxml())

    @timerdecorator
    def upload_file(
        self,
        filename,
        file_data: zipfile.ZipExtFile,
        kvp: List[Dict] | None,
        is_failed: bool = False,
    ):
        """
        Upload file to Filecloud and add metadata based on KVP

        Parameters
        ----------
            filename
                filename of the Searchable PDF file to be uploaded to Filecloud

            file_data
                PDF binary/bytes extracted from zipfile

            kvp
                Dict containing KVP information extracted from zipfile

        """
        # Determine where file will be uploaded
        folder_name = ""
        skriba_path = f"/{self.user}/Skriba"

        if type(file_data) == zipfile.ZipExtFile:
            pass
        if is_failed:
            folder_name = "Failed"
        else:
            folder_name = file_data.name.split("/")[-2]
            file_data.name = filename

        file_to_upload = {
            "file": file_data,
        }

        # Creates Folder. Does not overwrite existing folder
        create_folder_endpoint = "/core/createfolder"
        create_folder_response = self.session.post(
            self.server_url + create_folder_endpoint,
            params=self._create_folder_params(
                skriba_path,
                folder_name,
            ),
            cookies=self.cookies,
        )
        if folder_name == "Failed":
            pass

        print(create_folder_response.text)

        filecloud_path = f"{skriba_path}/{folder_name}"
        upload_api_params = self._create_upload_api_params(filecloud_path, filename)

        upload_endpoint = "/core/upload"
        upload_call = self.session.post(
            self.server_url + upload_endpoint,
            params=upload_api_params,
            files=file_to_upload,
            cookies=self.cookies,
        )
        if (upload_call.text) == "OK":
            print(
                bcolors.OKGREEN
                + f"Successfuly uploaded {filename} to Filecloud at {filecloud_path}"
                + bcolors.ENDC
            )
            if kvp:
                print(bcolors.OKCYAN + "Adding Metadata" + bcolors.ENDC)
                for _kvp in kvp:
                    self._add_metadata(filecloud_path, filename, _kvp)
            else:
                print(bcolors.WARNING + "No KVP found attached to file" + bcolors.ENDC)
        else:
            print(
                bcolors.FAIL
                + f"Something went wrong with uploading {filename}"
                + bcolors.ENDC
            )
            print(upload_call.text)
